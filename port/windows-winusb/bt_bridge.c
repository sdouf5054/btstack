/*
 * bt_bridge.c — Bluetooth Media Bridge
 *
 * Based on btstack a2dp_sink_demo.c by BlueKitchen GmbH.
 * Modified for bluetooth-media-bridge project:
 *   - TCP IPC server (port 9876) sends JSON events to Python GUI
 *   - Auto metadata request on track change
 *   - Auto cover art download on new image handle
 *   - Command reception from TCP clients (play/pause/next/prev/volume)
 *
 * Original copyright: BlueKitchen GmbH, non-commercial license.
 */

#define BTSTACK_FILE__ "bt_bridge.c"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#include "btstack.h"
#include "btstack_resample.h"

#ifdef HAVE_BTSTACK_STDIN
#include "btstack_stdin.h"
#endif

#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
#include "btstack_sample_rate_compensation.h"
#endif

#include "btstack_ring_buffer.h"

#ifdef HAVE_POSIX_FILE_IO
#include "wav_util.h"
#define STORE_TO_WAV_FILE
#endif

// ============================================================================
// IPC Server (Winsock TCP)
// ============================================================================
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#define SOCKET int
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close
#endif

#define IPC_PORT 9876
#define IPC_MAX_CLIENTS 4
#define IPC_SEND_BUF_SIZE 8192
#define COVER_ART_MAX_SIZE (512 * 1024)  // 512KB max cover art

static SOCKET ipc_server_socket = INVALID_SOCKET;
static SOCKET ipc_client_sockets[IPC_MAX_CLIENTS];
static int    ipc_num_clients = 0;
static bool   ipc_initialized = false;

// Cover art buffer for IPC transmission
static uint8_t  cover_art_buffer[COVER_ART_MAX_SIZE];
static uint32_t cover_art_buffer_offset = 0;
static bool     cover_art_collecting = false;

// Current metadata storage
static char current_title[256];
static char current_artist[256];
static char current_album[256];
static char current_genre[256];
static char current_image_handle[8];
static char current_playback_status[32];

// Deduplication: previous values to avoid redundant IPC sends / downloads
static char prev_image_handle[8];
static char prev_title[256];
static char prev_artist[256];
static bool metadata_complete = false;  // true when all fields received for current track
static uint32_t current_track_id = 0;
static uint32_t pending_coverart_track_id = 0;

static void ipc_init(void) {
#ifdef _WIN32
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        printf("IPC: WSAStartup failed\n");
        return;
    }
#endif
    for (int i = 0; i < IPC_MAX_CLIENTS; i++) {
        ipc_client_sockets[i] = INVALID_SOCKET;
    }

    ipc_server_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (ipc_server_socket == INVALID_SOCKET) {
        printf("IPC: socket() failed\n");
        return;
    }

    // Allow address reuse
    int opt = 1;
    setsockopt(ipc_server_socket, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    // Set non-blocking
#ifdef _WIN32
    u_long mode = 1;
    ioctlsocket(ipc_server_socket, FIONBIO, &mode);
#else
    int flags = fcntl(ipc_server_socket, F_GETFL, 0);
    fcntl(ipc_server_socket, F_SETFL, flags | O_NONBLOCK);
#endif

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(IPC_PORT);

    if (bind(ipc_server_socket, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        printf("IPC: bind() failed on port %d\n", IPC_PORT);
        closesocket(ipc_server_socket);
        ipc_server_socket = INVALID_SOCKET;
        return;
    }

    if (listen(ipc_server_socket, IPC_MAX_CLIENTS) == SOCKET_ERROR) {
        printf("IPC: listen() failed\n");
        closesocket(ipc_server_socket);
        ipc_server_socket = INVALID_SOCKET;
        return;
    }

    printf("IPC: Server listening on 127.0.0.1:%d\n", IPC_PORT);
    ipc_initialized = true;
}

static void ipc_accept_clients(void) {
    if (!ipc_initialized || ipc_server_socket == INVALID_SOCKET) return;

    struct sockaddr_in client_addr;
    int addr_len = sizeof(client_addr);
    SOCKET client = accept(ipc_server_socket, (struct sockaddr*)&client_addr, &addr_len);
    if (client != INVALID_SOCKET) {
        // Set non-blocking on client socket
#ifdef _WIN32
        u_long mode = 1;
        ioctlsocket(client, FIONBIO, &mode);
#else
        int flags = fcntl(client, F_GETFL, 0);
        fcntl(client, F_SETFL, flags | O_NONBLOCK);
#endif
        // Find empty slot
        for (int i = 0; i < IPC_MAX_CLIENTS; i++) {
            if (ipc_client_sockets[i] == INVALID_SOCKET) {
                ipc_client_sockets[i] = client;
                ipc_num_clients++;
                printf("IPC: Client connected (slot %d, total %d)\n", i, ipc_num_clients);
                return;
            }
        }
        // No slot available
        closesocket(client);
        printf("IPC: Client rejected (max clients reached)\n");
    }
}

// Send a JSON message to all connected IPC clients
static void ipc_send_json(const char *json) {
    if (!ipc_initialized) return;

    // Send length-prefixed: 4 bytes big-endian length + JSON + newline
    char buf[IPC_SEND_BUF_SIZE];
    int total = snprintf(buf, sizeof(buf), "%s\n", json);
    if (total >= (int)sizeof(buf)) total = (int)sizeof(buf) - 1;

    for (int i = 0; i < IPC_MAX_CLIENTS; i++) {
        if (ipc_client_sockets[i] == INVALID_SOCKET) continue;
        int sent = send(ipc_client_sockets[i], buf, total, 0);
        if (sent == SOCKET_ERROR) {
            // Client disconnected
            printf("IPC: Client disconnected (slot %d)\n", i);
            closesocket(ipc_client_sockets[i]);
            ipc_client_sockets[i] = INVALID_SOCKET;
            ipc_num_clients--;
        }
    }
}

// Send binary data (cover art) to all connected IPC clients
static void ipc_send_binary(const uint8_t *data, uint32_t size) {
    if (!ipc_initialized) return;

    for (int i = 0; i < IPC_MAX_CLIENTS; i++) {
        if (ipc_client_sockets[i] == INVALID_SOCKET) continue;
        int sent = send(ipc_client_sockets[i], (const char*)data, size, 0);
        if (sent == SOCKET_ERROR) {
            printf("IPC: Client disconnected during binary send (slot %d)\n", i);
            closesocket(ipc_client_sockets[i]);
            ipc_client_sockets[i] = INVALID_SOCKET;
            ipc_num_clients--;
        }
    }
}

// Simple JSON string escaper (handles \, ", newlines)
static void json_escape(const char *src, char *dst, int dst_size) {
    int j = 0;
    for (int i = 0; src[i] && j < dst_size - 2; i++) {
        switch (src[i]) {
            case '"':  if (j < dst_size - 3) { dst[j++] = '\\'; dst[j++] = '"'; } break;
            case '\\': if (j < dst_size - 3) { dst[j++] = '\\'; dst[j++] = '\\'; } break;
            case '\n': if (j < dst_size - 3) { dst[j++] = '\\'; dst[j++] = 'n'; } break;
            case '\r': if (j < dst_size - 3) { dst[j++] = '\\'; dst[j++] = 'r'; } break;
            case '\t': if (j < dst_size - 3) { dst[j++] = '\\'; dst[j++] = 't'; } break;
            default:   dst[j++] = src[i]; break;
        }
    }
    dst[j] = '\0';
}

// Send typed IPC events
static void ipc_send_event_connected(const char *addr) {
    char buf[256];
    snprintf(buf, sizeof(buf), "{\"type\":\"connected\",\"addr\":\"%s\"}", addr);
    ipc_send_json(buf);
}

static void ipc_send_event_disconnected(void) {
    ipc_send_json("{\"type\":\"disconnected\"}");
}

static void ipc_send_event_metadata(void) {
    char title_esc[512], artist_esc[512], album_esc[512], genre_esc[512];
    json_escape(current_title, title_esc, sizeof(title_esc));
    json_escape(current_artist, artist_esc, sizeof(artist_esc));
    json_escape(current_album, album_esc, sizeof(album_esc));
    json_escape(current_genre, genre_esc, sizeof(genre_esc));

    char buf[2048];
#ifdef ENABLE_AVRCP_COVER_ART
    snprintf(buf, sizeof(buf),
             "{\"type\":\"metadata\",\"title\":\"%s\",\"artist\":\"%s\",\"album\":\"%s\",\"genre\":\"%s\",\"cover_art_handle\":\"%s\",\"track_id\":%u}",
             title_esc, artist_esc, album_esc, genre_esc, current_image_handle, (unsigned)current_track_id);
#else
    snprintf(buf, sizeof(buf),
             "{\"type\":\"metadata\",\"title\":\"%s\",\"artist\":\"%s\",\"album\":\"%s\",\"genre\":\"%s\",\"cover_art_handle\":\"%s\"}",
             title_esc, artist_esc, album_esc, genre_esc, current_image_handle);
#endif
    ipc_send_json(buf);
}

static void ipc_send_event_playback(const char *status) {
    char buf[128];
    snprintf(buf, sizeof(buf), "{\"type\":\"playback\",\"status\":\"%s\"}", status);
    ipc_send_json(buf);
}

static void ipc_send_event_volume(int percent, int raw) {
    char buf[128];
    snprintf(buf, sizeof(buf), "{\"type\":\"volume\",\"percent\":%d,\"raw\":%d}", percent, raw);
    ipc_send_json(buf);
}

static void ipc_send_event_cover_art_start(uint32_t size) {
    char buf[128];
#ifdef ENABLE_AVRCP_COVER_ART
    snprintf(buf, sizeof(buf), "{\"type\":\"cover_art\",\"size\":%u,\"track_id\":%u}", (unsigned)size, (unsigned)pending_coverart_track_id);
#else
    snprintf(buf, sizeof(buf), "{\"type\":\"cover_art\",\"size\":%u}", (unsigned)size);
#endif
    ipc_send_json(buf);
}

static void ipc_send_event_stream_started(void) {
    ipc_send_json("{\"type\":\"stream_started\"}");
}

static void ipc_send_event_stream_stopped(void) {
    ipc_send_json("{\"type\":\"stream_stopped\"}");
}

static void ipc_send_event_btstack_ready(const char *addr) {
    char buf[256];
    snprintf(buf, sizeof(buf), "{\"type\":\"ready\",\"addr\":\"%s\"}", addr);
    ipc_send_json(buf);
}

// Forward declaration for command handling
static void ipc_handle_command(const char *cmd);

// Read commands from IPC clients (non-blocking)
static void ipc_poll_commands(void) {
    if (!ipc_initialized) return;

    ipc_accept_clients();

    char buf[512];
    for (int i = 0; i < IPC_MAX_CLIENTS; i++) {
        if (ipc_client_sockets[i] == INVALID_SOCKET) continue;
        int received = recv(ipc_client_sockets[i], buf, sizeof(buf) - 1, 0);
        if (received > 0) {
            buf[received] = '\0';
            // Process line-by-line
            char *line = strtok(buf, "\n");
            while (line) {
                ipc_handle_command(line);
                line = strtok(NULL, "\n");
            }
        } else if (received == 0) {
            // Client closed connection
            printf("IPC: Client closed connection (slot %d)\n", i);
            closesocket(ipc_client_sockets[i]);
            ipc_client_sockets[i] = INVALID_SOCKET;
            ipc_num_clients--;
        }
        // received < 0 is EWOULDBLOCK, which is fine for non-blocking
    }
}

static void ipc_close(void) {
    for (int i = 0; i < IPC_MAX_CLIENTS; i++) {
        if (ipc_client_sockets[i] != INVALID_SOCKET) {
            closesocket(ipc_client_sockets[i]);
            ipc_client_sockets[i] = INVALID_SOCKET;
        }
    }
    if (ipc_server_socket != INVALID_SOCKET) {
        closesocket(ipc_server_socket);
        ipc_server_socket = INVALID_SOCKET;
    }
#ifdef _WIN32
    WSACleanup();
#endif
    ipc_initialized = false;
}


// ============================================================================
// Audio / A2DP / AVRCP (based on a2dp_sink_demo.c)
// ============================================================================

#define NUM_CHANNELS 2
#define BYTES_PER_FRAME     (2*NUM_CHANNELS)
#define MAX_SBC_FRAME_SIZE 120

static bd_addr_t device_addr;

#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
static btstack_sample_rate_compensation_t sample_rate_compensation;
#endif

static btstack_packet_callback_registration_t hci_event_callback_registration;

static uint8_t  sdp_avdtp_sink_service_buffer[150];
static uint8_t  sdp_avrcp_target_service_buffer[150];
static uint8_t  sdp_avrcp_controller_service_buffer[200];
static uint8_t  device_id_sdp_service_buffer[100];

static uint8_t media_sbc_codec_capabilities[] = {
    0xFF, 0xFF, 2, 53
};

#ifdef STORE_TO_WAV_FILE
static uint32_t audio_frame_count = 0;
static char * wav_filename = "bt_bridge_audio.wav";
#endif

static const btstack_sbc_decoder_t *   sbc_decoder_instance;
static btstack_sbc_decoder_bluedroid_t sbc_decoder_context;

#define OPTIMAL_FRAMES_MIN 60
#define OPTIMAL_FRAMES_MAX 80
#define ADDITIONAL_FRAMES  30
static uint8_t sbc_frame_storage[(OPTIMAL_FRAMES_MAX + ADDITIONAL_FRAMES) * MAX_SBC_FRAME_SIZE];
static btstack_ring_buffer_t sbc_frame_ring_buffer;
static unsigned int sbc_frame_size;

static uint8_t decoded_audio_storage[(128+16) * BYTES_PER_FRAME];
static btstack_ring_buffer_t decoded_audio_ring_buffer;

static int media_initialized = 0;
static int audio_stream_started;
#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
static int l2cap_stream_started;
#endif
static btstack_resample_t resample_instance;

static int16_t * request_buffer;
static int       request_frames;

static int volume_percentage = 0;
static avrcp_battery_status_t battery_status = AVRCP_BATTERY_STATUS_WARNING;

#ifdef ENABLE_AVRCP_COVER_ART
static char a2dp_sink_demo_image_handle[8];
static avrcp_cover_art_client_t a2dp_sink_demo_cover_art_client;
static bool a2dp_sink_demo_cover_art_client_connected;
static uint16_t a2dp_sink_demo_cover_art_cid;
static uint8_t a2dp_sink_demo_ertm_buffer[2000];
static l2cap_ertm_config_t a2dp_sink_demo_ertm_config = {
    1, 2, 2000, 12000, 512, 2, 2, 1,
};
static bool a2dp_sink_cover_art_download_active;
static uint32_t a2dp_sink_cover_art_file_size;
static const char * a2dp_sink_demo_thumbnail_path = "cover.jpg";
static FILE * a2dp_sink_cover_art_file;
// Flag to auto-download cover art when handle is received
static bool auto_download_cover_art_pending = false;
#endif


typedef struct {
    uint8_t  reconfigure;
    uint8_t  num_channels;
    uint16_t sampling_frequency;
    uint8_t  block_length;
    uint8_t  subbands;
    uint8_t  min_bitpool_value;
    uint8_t  max_bitpool_value;
    btstack_sbc_channel_mode_t      channel_mode;
    btstack_sbc_allocation_method_t allocation_method;
} media_codec_configuration_sbc_t;

typedef enum {
    STREAM_STATE_CLOSED,
    STREAM_STATE_OPEN,
    STREAM_STATE_PLAYING,
    STREAM_STATE_PAUSED,
} stream_state_t;

typedef struct {
    uint8_t  a2dp_local_seid;
    uint8_t  media_sbc_codec_configuration[4];
} a2dp_sink_demo_stream_endpoint_t;
static a2dp_sink_demo_stream_endpoint_t a2dp_sink_demo_stream_endpoint;

typedef struct {
    bd_addr_t addr;
    uint16_t  a2dp_cid;
    uint8_t   a2dp_local_seid;
    stream_state_t stream_state;
    media_codec_configuration_sbc_t sbc_configuration;
} a2dp_sink_demo_a2dp_connection_t;
static a2dp_sink_demo_a2dp_connection_t a2dp_sink_demo_a2dp_connection;

typedef struct {
    bd_addr_t addr;
    uint16_t  avrcp_cid;
    bool playing;
    uint16_t notifications_supported_by_target;
} a2dp_sink_demo_avrcp_connection_t;
static a2dp_sink_demo_avrcp_connection_t a2dp_sink_demo_avrcp_connection;

// Timer for IPC polling
static btstack_timer_source_t ipc_poll_timer;
#define IPC_POLL_INTERVAL_MS 50

// Forward declarations
static void hci_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size);
static void a2dp_sink_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t * packet, uint16_t event_size);
static void handle_l2cap_media_data_packet(uint8_t seid, uint8_t *packet, uint16_t size);
static void avrcp_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size);
static void avrcp_controller_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size);
static void avrcp_target_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size);
#ifdef HAVE_BTSTACK_STDIN
static void stdin_process(char cmd);
#endif

// Auto-trigger cover art download
#ifdef ENABLE_AVRCP_COVER_ART
static void trigger_cover_art_download(void);
static uint8_t a2dp_sink_demo_cover_art_connect(void);
#endif

// ============================================================================
// IPC Command Handler
// ============================================================================

static void ipc_handle_command(const char *cmd) {
    a2dp_sink_demo_avrcp_connection_t *avrcp = &a2dp_sink_demo_avrcp_connection;
    if (avrcp->avrcp_cid == 0) {
        printf("IPC: Command '%s' ignored - no AVRCP connection\n", cmd);
        return;
    }

    // Simple command matching (JSON-like: {"cmd":"play"})
    if (strstr(cmd, "\"play\"")) {
        printf("IPC: play\n");
        avrcp_controller_play(avrcp->avrcp_cid);
    } else if (strstr(cmd, "\"pause\"")) {
        printf("IPC: pause\n");
        avrcp_controller_pause(avrcp->avrcp_cid);
    } else if (strstr(cmd, "\"stop\"")) {
        printf("IPC: stop\n");
        avrcp_controller_stop(avrcp->avrcp_cid);
    } else if (strstr(cmd, "\"next\"")) {
        printf("IPC: next\n");
        avrcp_controller_forward(avrcp->avrcp_cid);
    } else if (strstr(cmd, "\"prev\"")) {
        printf("IPC: prev\n");
        avrcp_controller_backward(avrcp->avrcp_cid);
    } else if (strstr(cmd, "\"volume_up\"")) {
        volume_percentage = volume_percentage <= 90 ? volume_percentage + 10 : 100;
        uint8_t volume = volume_percentage * 127 / 100;
        avrcp_target_volume_changed(avrcp->avrcp_cid, volume);
        printf("IPC: volume up to %d%%\n", volume_percentage);
    } else if (strstr(cmd, "\"volume_down\"")) {
        volume_percentage = volume_percentage >= 10 ? volume_percentage - 10 : 0;
        uint8_t volume = volume_percentage * 127 / 100;
        avrcp_target_volume_changed(avrcp->avrcp_cid, volume);
        printf("IPC: volume down to %d%%\n", volume_percentage);
    } else if (strstr(cmd, "\"get_metadata\"")) {
        printf("IPC: get_metadata\n");
        avrcp_controller_get_now_playing_info(avrcp->avrcp_cid);
    } else {
        printf("IPC: Unknown command: %s\n", cmd);
    }
}


// ============================================================================
// IPC Poll Timer (integrated with btstack run loop)
// ============================================================================

static void ipc_poll_timer_handler(btstack_timer_source_t *ts) {
    ipc_poll_commands();

    // Re-register timer
    btstack_run_loop_set_timer(ts, IPC_POLL_INTERVAL_MS);
    btstack_run_loop_add_timer(ts);
}


// ============================================================================
// Audio Processing (unchanged from a2dp_sink_demo.c)
// ============================================================================

static void playback_handler(int16_t * buffer, uint16_t num_audio_frames, const btstack_audio_context_t * context){
    UNUSED(context);
#ifdef STORE_TO_WAV_FILE
    int       wav_samples = num_audio_frames * NUM_CHANNELS;
    int16_t * wav_buffer  = buffer;
#endif
    if (sbc_frame_size == 0){
        memset(buffer, 0, num_audio_frames * BYTES_PER_FRAME);
        return;
    }
    uint32_t bytes_read;
    btstack_ring_buffer_read(&decoded_audio_ring_buffer, (uint8_t *) buffer, num_audio_frames * BYTES_PER_FRAME, &bytes_read);
    buffer          += bytes_read / NUM_CHANNELS;
    num_audio_frames   -= bytes_read / BYTES_PER_FRAME;
    request_buffer = buffer;
    request_frames = num_audio_frames;
    while (request_frames && btstack_ring_buffer_bytes_available(&sbc_frame_ring_buffer) >= sbc_frame_size){
        uint8_t sbc_frame[MAX_SBC_FRAME_SIZE];
        btstack_ring_buffer_read(&sbc_frame_ring_buffer, sbc_frame, sbc_frame_size, &bytes_read);
        sbc_decoder_instance->decode_signed_16(&sbc_decoder_context, 0, sbc_frame, sbc_frame_size);
    }
#ifdef STORE_TO_WAV_FILE
    audio_frame_count += num_audio_frames;
    wav_writer_write_int16(wav_samples, wav_buffer);
#endif
}

static void handle_pcm_data(int16_t * data, int num_audio_frames, int num_channels, int sample_rate, void * context){
    UNUSED(sample_rate);
    UNUSED(context);
    UNUSED(num_channels);
    const btstack_audio_sink_t * audio_sink = btstack_audio_sink_get_instance();
    if (!audio_sink){
#ifdef STORE_TO_WAV_FILE
        audio_frame_count += num_audio_frames;
        wav_writer_write_int16(num_audio_frames * NUM_CHANNELS, data);
#endif
        return;
    }
    int16_t  output_buffer[(128+16) * NUM_CHANNELS];
    uint32_t resampled_frames = btstack_resample_block(&resample_instance, data, num_audio_frames, output_buffer);
    int frames_to_copy = btstack_min(resampled_frames, request_frames);
    memcpy(request_buffer, output_buffer, frames_to_copy * BYTES_PER_FRAME);
    request_frames  -= frames_to_copy;
    request_buffer  += frames_to_copy * NUM_CHANNELS;
    int frames_to_store = resampled_frames - frames_to_copy;
    if (frames_to_store){
        int status = btstack_ring_buffer_write(&decoded_audio_ring_buffer, (uint8_t *)&output_buffer[frames_to_copy * NUM_CHANNELS], frames_to_store * BYTES_PER_FRAME);
        if (status){
            printf("Error storing samples in PCM ring buffer!!!\n");
        }
    }
}

static int media_processing_init(media_codec_configuration_sbc_t * configuration){
    if (media_initialized) return 0;
    sbc_decoder_instance = btstack_sbc_decoder_bluedroid_init_instance(&sbc_decoder_context);
    sbc_decoder_instance->configure(&sbc_decoder_context, SBC_MODE_STANDARD, handle_pcm_data, NULL);
#ifdef STORE_TO_WAV_FILE
    wav_writer_open(wav_filename, configuration->num_channels, configuration->sampling_frequency);
#endif
    btstack_ring_buffer_init(&sbc_frame_ring_buffer, sbc_frame_storage, sizeof(sbc_frame_storage));
    btstack_ring_buffer_init(&decoded_audio_ring_buffer, decoded_audio_storage, sizeof(decoded_audio_storage));
    btstack_resample_init(&resample_instance, configuration->num_channels);
    const btstack_audio_sink_t * audio = btstack_audio_sink_get_instance();
    if (audio){
        audio->init(NUM_CHANNELS, configuration->sampling_frequency, &playback_handler);
    }
    audio_stream_started = 0;
    media_initialized = 1;
    return 0;
}

static void media_processing_start(void){
    if (!media_initialized) return;
#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
    btstack_sample_rate_compensation_reset(&sample_rate_compensation, btstack_run_loop_get_time_ms());
#endif
    const btstack_audio_sink_t * audio = btstack_audio_sink_get_instance();
    if (audio){
        audio->start_stream();
    }
    audio_stream_started = 1;
}

static void media_processing_pause(void){
    if (!media_initialized) return;
    audio_stream_started = 0;
#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
    l2cap_stream_started = 0;
#endif
    const btstack_audio_sink_t * audio = btstack_audio_sink_get_instance();
    if (audio){
        audio->stop_stream();
    }
    btstack_ring_buffer_reset(&decoded_audio_ring_buffer);
    btstack_ring_buffer_reset(&sbc_frame_ring_buffer);
}

static void media_processing_close(void){
    if (!media_initialized) return;
    media_initialized = 0;
    audio_stream_started = 0;
    sbc_frame_size = 0;
#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
    l2cap_stream_started = 0;
#endif
#ifdef STORE_TO_WAV_FILE
    wav_writer_close();
    printf("WAV Writer: Wrote %u audio frames to %s\n", audio_frame_count, wav_filename);
#endif
    const btstack_audio_sink_t * audio = btstack_audio_sink_get_instance();
    if (audio){
        audio->close();
    }
}

static void dump_sbc_configuration(media_codec_configuration_sbc_t * configuration){
    printf("    - num_channels: %d\n", configuration->num_channels);
    printf("    - sampling_frequency: %d\n", configuration->sampling_frequency);
    printf("    - channel_mode: %d\n", configuration->channel_mode);
    printf("    - block_length: %d\n", configuration->block_length);
    printf("    - subbands: %d\n", configuration->subbands);
    printf("    - allocation_method: %d\n", configuration->allocation_method);
    printf("    - bitpool_value [%d, %d] \n", configuration->min_bitpool_value, configuration->max_bitpool_value);
}


// ============================================================================
// Media Data Handling (unchanged from a2dp_sink_demo.c)
// ============================================================================

static int read_media_data_header(uint8_t *packet, int size, int *offset, avdtp_media_packet_header_t *media_header){
    int media_header_len = 12;
    int pos = *offset;
    if (size - pos < media_header_len) return 0;
    media_header->version = packet[pos] & 0x03;
    media_header->padding = get_bit16(packet[pos],2);
    media_header->extension = get_bit16(packet[pos],3);
    media_header->csrc_count = (packet[pos] >> 4) & 0x0F;
    pos++;
    media_header->marker = get_bit16(packet[pos],0);
    media_header->payload_type  = (packet[pos] >> 1) & 0x7F;
    pos++;
    media_header->sequence_number = big_endian_read_16(packet, pos);
    pos+=2;
    media_header->timestamp = big_endian_read_32(packet, pos);
    pos+=4;
    media_header->synchronization_source = big_endian_read_32(packet, pos);
    pos+=4;
    *offset = pos;
    return 1;
}

static int read_sbc_header(uint8_t * packet, int size, int * offset, avdtp_sbc_codec_header_t * sbc_header){
    int sbc_header_len = 12;
    int pos = *offset;
    if (size - pos < sbc_header_len) return 0;
    sbc_header->fragmentation = get_bit16(packet[pos], 7);
    sbc_header->starting_packet = get_bit16(packet[pos], 6);
    sbc_header->last_packet = get_bit16(packet[pos], 5);
    sbc_header->num_frames = packet[pos] & 0x0f;
    pos++;
    *offset = pos;
    return 1;
}

static void handle_l2cap_media_data_packet(uint8_t seid, uint8_t *packet, uint16_t size){
    UNUSED(seid);
    int pos = 0;
    avdtp_media_packet_header_t media_header;
    if (!read_media_data_header(packet, size, &pos, &media_header)) return;
    avdtp_sbc_codec_header_t sbc_header;
    if (!read_sbc_header(packet, size, &pos, &sbc_header)) return;
    int packet_length = size-pos;
    uint8_t *packet_begin = packet+pos;
    const btstack_audio_sink_t * audio = btstack_audio_sink_get_instance();
    if (!audio){
        sbc_decoder_instance->decode_signed_16(&sbc_decoder_context, 0, packet_begin, packet_length);
        return;
    }
    sbc_frame_size = packet_length / sbc_header.num_frames;
    int status = btstack_ring_buffer_write(&sbc_frame_ring_buffer, packet_begin, packet_length);
    if (status != ERROR_CODE_SUCCESS){
        printf("Error storing samples in SBC ring buffer!!!\n");
    }
    int sbc_frames_in_buffer = btstack_ring_buffer_bytes_available(&sbc_frame_ring_buffer) / sbc_frame_size;
#ifdef HAVE_BTSTACK_AUDIO_EFFECTIVE_SAMPLERATE
    if (!l2cap_stream_started && audio_stream_started) {
        l2cap_stream_started = 1;
        btstack_sample_rate_compensation_init(&sample_rate_compensation, btstack_run_loop_get_time_ms(), a2dp_sink_demo_a2dp_connection.sbc_configuration.sampling_frequency, FLOAT_TO_Q15(1.f));
    }
    if (audio_stream_started && (audio != NULL)) {
        uint32_t resampling_factor = btstack_sample_rate_compensation_update(&sample_rate_compensation, btstack_run_loop_get_time_ms(), sbc_header.num_frames*128, audio->get_samplerate());
        btstack_resample_set_factor(&resample_instance, resampling_factor);
    }
#else
    uint32_t resampling_factor;
    uint32_t nominal_factor = 0x10000;
    uint32_t compensation   = 0x00100;
    if (sbc_frames_in_buffer < OPTIMAL_FRAMES_MIN){
        resampling_factor = nominal_factor - compensation;
    } else if (sbc_frames_in_buffer <= OPTIMAL_FRAMES_MAX){
        resampling_factor = nominal_factor;
    } else {
        resampling_factor = nominal_factor + compensation;
    }
    btstack_resample_set_factor(&resample_instance, resampling_factor);
#endif
    if (!audio_stream_started && sbc_frames_in_buffer >= OPTIMAL_FRAMES_MIN){
        media_processing_start();
    }
}


// ============================================================================
// HCI Packet Handler
// ============================================================================

static void hci_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size){
    UNUSED(channel);
    UNUSED(size);
    if (packet_type != HCI_EVENT_PACKET) return;
    if (hci_event_packet_get_type(packet) == HCI_EVENT_PIN_CODE_REQUEST) {
        bd_addr_t address;
        printf("Pin code request - using '0000'\n");
        hci_event_pin_code_request_get_bd_addr(packet, address);
        gap_pin_code_response(address, "0000");
    }
}


// ============================================================================
// Cover Art Handler
// ============================================================================

#ifdef ENABLE_AVRCP_COVER_ART
static void a2dp_sink_demo_cover_art_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size) {
    UNUSED(channel);
    UNUSED(size);
    uint8_t status;
    uint16_t cid;
    switch (packet_type){
        case BIP_DATA_PACKET:
            if (a2dp_sink_cover_art_download_active){
                a2dp_sink_cover_art_file_size += size;
                // Write to file
                if (a2dp_sink_cover_art_file) {
                    fwrite(packet, 1, size, a2dp_sink_cover_art_file);
                }
                // Also collect in buffer for IPC
                if (cover_art_collecting && (cover_art_buffer_offset + size) <= COVER_ART_MAX_SIZE) {
                    memcpy(cover_art_buffer + cover_art_buffer_offset, packet, size);
                    cover_art_buffer_offset += size;
                }
            }
            break;
        case HCI_EVENT_PACKET:
            switch (hci_event_packet_get_type(packet)){
                case HCI_EVENT_AVRCP_META:
                    switch (hci_event_avrcp_meta_get_subevent_code(packet)){
                        case AVRCP_SUBEVENT_COVER_ART_CONNECTION_ESTABLISHED:
                            status = avrcp_subevent_cover_art_connection_established_get_status(packet);
                            cid = avrcp_subevent_cover_art_connection_established_get_cover_art_cid(packet);
                            if (status == ERROR_CODE_SUCCESS){
                                printf("Cover Art: connection established, cid 0x%02x\n", cid);
                                a2dp_sink_demo_cover_art_client_connected = true;
                            } else {
                                printf("Cover Art: connection failed, status 0x%02x\n", status);
                                a2dp_sink_demo_cover_art_cid = 0;
                            }
                            break;
                        case AVRCP_SUBEVENT_COVER_ART_OPERATION_COMPLETE:
                            if (a2dp_sink_cover_art_download_active){
                                a2dp_sink_cover_art_download_active = false;
                                printf("Cover Art: download complete, %u bytes\n", a2dp_sink_cover_art_file_size);
                                if (a2dp_sink_cover_art_file) {
                                    fclose(a2dp_sink_cover_art_file);
                                    a2dp_sink_cover_art_file = NULL;
                                }
                                // Guard: only send to GUI if the cover art belongs to the current track
                                if (pending_coverart_track_id == current_track_id && cover_art_collecting && cover_art_buffer_offset > 0) {
                                    ipc_send_event_cover_art_start(cover_art_buffer_offset);
                                    ipc_send_binary(cover_art_buffer, cover_art_buffer_offset);
                                    printf("Cover Art: sent %u bytes for track %u\n", cover_art_buffer_offset, current_track_id);
                                } else {
                                    printf("Cover Art: discarded stale art (track %u vs current %u)\n",
                                           pending_coverart_track_id, current_track_id);
                                }
                                cover_art_collecting = false;
                                cover_art_buffer_offset = 0;
                                pending_coverart_track_id = 0;
                            }
                            break;
                        case AVRCP_SUBEVENT_COVER_ART_CONNECTION_RELEASED:
                            a2dp_sink_demo_cover_art_client_connected = false;
                            a2dp_sink_demo_cover_art_cid = 0;
                            printf("Cover Art: connection released\n");
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }
}

static uint8_t a2dp_sink_demo_cover_art_connect(void) {
    uint8_t status;
    status = avrcp_cover_art_client_connect(&a2dp_sink_demo_cover_art_client, a2dp_sink_demo_cover_art_packet_handler,
                                            device_addr, a2dp_sink_demo_ertm_buffer,
                                            sizeof(a2dp_sink_demo_ertm_buffer), &a2dp_sink_demo_ertm_config,
                                            &a2dp_sink_demo_cover_art_cid);
    return status;
}

static void trigger_cover_art_download(void) {
    if (!a2dp_sink_demo_cover_art_client_connected) return;
    if (a2dp_sink_cover_art_download_active) return;
    if (a2dp_sink_demo_image_handle[0] == '\0') return;

    printf("Cover Art: auto-downloading '%s'\n", a2dp_sink_demo_image_handle);

    // Open file for writing
    a2dp_sink_cover_art_file = fopen(a2dp_sink_demo_thumbnail_path, "wb");

    // Prepare IPC buffer
    cover_art_collecting = true;
    cover_art_buffer_offset = 0;

    a2dp_sink_cover_art_download_active = true;
    a2dp_sink_cover_art_file_size = 0;
    avrcp_cover_art_client_get_linked_thumbnail(a2dp_sink_demo_cover_art_cid, a2dp_sink_demo_image_handle);
}
#endif


// ============================================================================
// AVRCP Handlers
// ============================================================================

static void avrcp_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size){
    UNUSED(channel);
    UNUSED(size);
    uint16_t local_cid;
    uint8_t  status;
    bd_addr_t address;

    a2dp_sink_demo_avrcp_connection_t * connection = &a2dp_sink_demo_avrcp_connection;

    if (packet_type != HCI_EVENT_PACKET) return;
    if (hci_event_packet_get_type(packet) != HCI_EVENT_AVRCP_META) return;
    switch (packet[2]){
        case AVRCP_SUBEVENT_CONNECTION_ESTABLISHED: {
            local_cid = avrcp_subevent_connection_established_get_avrcp_cid(packet);
            status = avrcp_subevent_connection_established_get_status(packet);
            if (status != ERROR_CODE_SUCCESS){
                printf("AVRCP: Connection failed, status 0x%02x\n", status);
                connection->avrcp_cid = 0;
                return;
            }
            connection->avrcp_cid = local_cid;
            avrcp_subevent_connection_established_get_bd_addr(packet, address);
            printf("AVRCP: Connected to %s, cid 0x%02x\n", bd_addr_to_str(address), connection->avrcp_cid);

            // Store address for outgoing connections
            avrcp_subevent_connection_established_get_bd_addr(packet, device_addr);

            avrcp_target_support_event(connection->avrcp_cid, AVRCP_NOTIFICATION_EVENT_VOLUME_CHANGED);
            avrcp_target_support_event(connection->avrcp_cid, AVRCP_NOTIFICATION_EVENT_BATT_STATUS_CHANGED);
            avrcp_target_battery_status_changed(connection->avrcp_cid, battery_status);
            avrcp_controller_get_supported_events(connection->avrcp_cid);

            // IPC: notify connection
            ipc_send_event_connected(bd_addr_to_str(address));
            return;
        }
        case AVRCP_SUBEVENT_CONNECTION_RELEASED:
            printf("AVRCP: Channel released: cid 0x%02x\n", avrcp_subevent_connection_released_get_avrcp_cid(packet));
            connection->avrcp_cid = 0;
            connection->notifications_supported_by_target = 0;
            ipc_send_event_disconnected();
            return;
        default:
            break;
    }
}

static void avrcp_controller_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size){
    UNUSED(channel);
    UNUSED(size);

    uint8_t avrcp_subevent_value[256];
    uint8_t play_status;
    uint8_t event_id;

    a2dp_sink_demo_avrcp_connection_t * avrcp_connection = &a2dp_sink_demo_avrcp_connection;

    if (packet_type != HCI_EVENT_PACKET) return;
    if (hci_event_packet_get_type(packet) != HCI_EVENT_AVRCP_META) return;
    if (avrcp_connection->avrcp_cid == 0) return;

    memset(avrcp_subevent_value, 0, sizeof(avrcp_subevent_value));
    switch (packet[2]){
        case AVRCP_SUBEVENT_GET_CAPABILITY_EVENT_ID:
            avrcp_connection->notifications_supported_by_target |= (1 << avrcp_subevent_get_capability_event_id_get_event_id(packet));
            break;
        case AVRCP_SUBEVENT_GET_CAPABILITY_EVENT_ID_DONE:
            printf("AVRCP Controller: capabilities received, enabling notifications\n");
            avrcp_controller_enable_notification(avrcp_connection->avrcp_cid, AVRCP_NOTIFICATION_EVENT_PLAYBACK_STATUS_CHANGED);
            avrcp_controller_enable_notification(avrcp_connection->avrcp_cid, AVRCP_NOTIFICATION_EVENT_NOW_PLAYING_CONTENT_CHANGED);
            avrcp_controller_enable_notification(avrcp_connection->avrcp_cid, AVRCP_NOTIFICATION_EVENT_TRACK_CHANGED);
#ifdef ENABLE_AVRCP_COVER_ART
            avrcp_controller_enable_notification(a2dp_sink_demo_avrcp_connection.avrcp_cid, AVRCP_NOTIFICATION_EVENT_UIDS_CHANGED);
            a2dp_sink_demo_cover_art_connect();
#endif
            break;

        case AVRCP_SUBEVENT_NOTIFICATION_PLAYBACK_STATUS_CHANGED:
            play_status = avrcp_subevent_notification_playback_status_changed_get_play_status(packet);
            switch (play_status){
                case AVRCP_PLAYBACK_STATUS_PLAYING:
                    avrcp_connection->playing = true;
                    snprintf(current_playback_status, sizeof(current_playback_status), "playing");
                    break;
                case AVRCP_PLAYBACK_STATUS_PAUSED:
                    avrcp_connection->playing = false;
                    snprintf(current_playback_status, sizeof(current_playback_status), "paused");
                    break;
                case AVRCP_PLAYBACK_STATUS_STOPPED:
                    avrcp_connection->playing = false;
                    snprintf(current_playback_status, sizeof(current_playback_status), "stopped");
                    break;
                default:
                    avrcp_connection->playing = false;
                    snprintf(current_playback_status, sizeof(current_playback_status), "unknown");
                    break;
            }
            printf("AVRCP: Playback %s\n", current_playback_status);
            ipc_send_event_playback(current_playback_status);
            break;

        case AVRCP_SUBEVENT_NOTIFICATION_TRACK_CHANGED:
            printf("AVRCP: Track changed\n");
            // New track: bump ID so any in-flight cover art download is invalidated
            current_track_id++;
            // Reset metadata for new track
            metadata_complete = false;
            memset(current_title, 0, sizeof(current_title));
            memset(current_artist, 0, sizeof(current_artist));
            memset(current_album, 0, sizeof(current_album));
            memset(current_genre, 0, sizeof(current_genre));
            memset(current_image_handle, 0, sizeof(current_image_handle));
#ifdef ENABLE_AVRCP_COVER_ART
            // Invalidate any in-progress cover art download for the previous track
            auto_download_cover_art_pending = false;
            cover_art_collecting = false;
            cover_art_buffer_offset = 0;
            a2dp_sink_cover_art_download_active = false;
            pending_coverart_track_id = 0;
#endif
            // Auto-request metadata on track change
            avrcp_controller_get_now_playing_info(avrcp_connection->avrcp_cid);
            break;

        case AVRCP_SUBEVENT_NOTIFICATION_NOW_PLAYING_CONTENT_CHANGED:
            printf("AVRCP: Playing content changed\n");
            // Only request if we haven't already gotten metadata for this track
            if (metadata_complete) {
                metadata_complete = false;
                avrcp_controller_get_now_playing_info(avrcp_connection->avrcp_cid);
            }
            break;

        case AVRCP_SUBEVENT_NOW_PLAYING_TITLE_INFO:
            if (avrcp_subevent_now_playing_title_info_get_value_len(packet) > 0){
                memcpy(avrcp_subevent_value, avrcp_subevent_now_playing_title_info_get_value(packet), avrcp_subevent_now_playing_title_info_get_value_len(packet));
                snprintf(current_title, sizeof(current_title), "%s", (char*)avrcp_subevent_value);
                printf("AVRCP: Title: %s\n", current_title);
            }
            break;

        case AVRCP_SUBEVENT_NOW_PLAYING_ARTIST_INFO:
            if (avrcp_subevent_now_playing_artist_info_get_value_len(packet) > 0){
                memcpy(avrcp_subevent_value, avrcp_subevent_now_playing_artist_info_get_value(packet), avrcp_subevent_now_playing_artist_info_get_value_len(packet));
                snprintf(current_artist, sizeof(current_artist), "%s", (char*)avrcp_subevent_value);
                printf("AVRCP: Artist: %s\n", current_artist);
            }
            break;

        case AVRCP_SUBEVENT_NOW_PLAYING_ALBUM_INFO:
            if (avrcp_subevent_now_playing_album_info_get_value_len(packet) > 0){
                memcpy(avrcp_subevent_value, avrcp_subevent_now_playing_album_info_get_value(packet), avrcp_subevent_now_playing_album_info_get_value_len(packet));
                snprintf(current_album, sizeof(current_album), "%s", (char*)avrcp_subevent_value);
                printf("AVRCP: Album: %s\n", current_album);
            }
            break;

        case AVRCP_SUBEVENT_NOW_PLAYING_GENRE_INFO:
            if (avrcp_subevent_now_playing_genre_info_get_value_len(packet) > 0){
                memcpy(avrcp_subevent_value, avrcp_subevent_now_playing_genre_info_get_value(packet), avrcp_subevent_now_playing_genre_info_get_value_len(packet));
                snprintf(current_genre, sizeof(current_genre), "%s", (char*)avrcp_subevent_value);
                printf("AVRCP: Genre: %s\n", current_genre);
            }
            break;

#ifdef ENABLE_AVRCP_COVER_ART
        case AVRCP_SUBEVENT_NOW_PLAYING_COVER_ART_INFO:
            if (avrcp_subevent_now_playing_cover_art_info_get_value_len(packet) == 7){
                memcpy(a2dp_sink_demo_image_handle, avrcp_subevent_now_playing_cover_art_info_get_value(packet), 7);
                memcpy(current_image_handle, a2dp_sink_demo_image_handle, 8);
                printf("AVRCP: Cover Art handle: %s\n", a2dp_sink_demo_image_handle);

                // Dedup: send metadata + download if title, artist, OR handle changed
                bool title_changed  = (strcmp(current_title,        prev_title)         != 0);
                bool artist_changed = (strcmp(current_artist,       prev_artist)        != 0);
                bool handle_changed = (strcmp(current_image_handle, prev_image_handle)  != 0);

                if (title_changed || artist_changed || handle_changed) {
                    // New track — send metadata and queue cover art download
                    metadata_complete = true;
                    memcpy(prev_title,        current_title,        sizeof(prev_title));
                    memcpy(prev_artist,       current_artist,       sizeof(prev_artist));
                    memcpy(prev_image_handle, current_image_handle, sizeof(prev_image_handle));
                    ipc_send_event_metadata();
                    if (handle_changed) {
                        // Stamp this download request with the current track ID
                        pending_coverart_track_id = current_track_id;
                        auto_download_cover_art_pending = true;
                    }
                    printf("AVRCP: New track detected, sending metadata\n");
                } else {
                    printf("AVRCP: Duplicate track notification, skipping\n");
                }
            }
            break;

        case AVRCP_SUBEVENT_NOTIFICATION_EVENT_UIDS_CHANGED:
            if (a2dp_sink_demo_cover_art_client_connected){
                printf("AVRCP: UIDs changed -> disconnect cover art client\n");
                avrcp_cover_art_client_disconnect(a2dp_sink_demo_cover_art_cid);
            }
            break;
#endif

        case AVRCP_SUBEVENT_NOW_PLAYING_TRACK_INFO:
        case AVRCP_SUBEVENT_NOW_PLAYING_TOTAL_TRACKS_INFO:
            // Ignored for IPC purposes
            break;

        case AVRCP_SUBEVENT_NOTIFICATION_STATE:
            event_id = (avrcp_notification_event_id_t)avrcp_subevent_notification_state_get_event_id(packet);
            printf("AVRCP: %s notification registered\n", avrcp_notification2str(event_id));
            break;

        case AVRCP_SUBEVENT_OPERATION_COMPLETE:
            break;

        case AVRCP_SUBEVENT_PLAY_STATUS:
            break;

        default:
            break;
    }

    // Handle deferred cover art download (outside switch to avoid timing issues)
#ifdef ENABLE_AVRCP_COVER_ART
    if (auto_download_cover_art_pending && a2dp_sink_demo_cover_art_client_connected && !a2dp_sink_cover_art_download_active) {
        auto_download_cover_art_pending = false;
        trigger_cover_art_download();
    }
#endif
}

static void avrcp_volume_changed(uint8_t volume){
    const btstack_audio_sink_t * audio = btstack_audio_sink_get_instance();
    if (audio){
        audio->set_volume(volume);
    }
}

static void avrcp_target_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size){
    UNUSED(channel);
    UNUSED(size);
    if (packet_type != HCI_EVENT_PACKET) return;
    if (hci_event_packet_get_type(packet) != HCI_EVENT_AVRCP_META) return;

    uint8_t volume;
    switch (packet[2]){
        case AVRCP_SUBEVENT_NOTIFICATION_VOLUME_CHANGED:
            volume = avrcp_subevent_notification_volume_changed_get_absolute_volume(packet);
            volume_percentage = volume * 100 / 127;
            printf("AVRCP: Volume %d%% (%d)\n", volume_percentage, volume);
            avrcp_volume_changed(volume);
            ipc_send_event_volume(volume_percentage, volume);
            break;
        case AVRCP_SUBEVENT_OPERATION:
            break;
        default:
            break;
    }
}


// ============================================================================
// A2DP Sink Packet Handler
// ============================================================================

static void a2dp_sink_packet_handler(uint8_t packet_type, uint16_t channel, uint8_t *packet, uint16_t size){
    UNUSED(channel);
    UNUSED(size);
    uint8_t status;
    uint8_t allocation_method;

    if (packet_type != HCI_EVENT_PACKET) return;
    if (hci_event_packet_get_type(packet) != HCI_EVENT_A2DP_META) return;

    a2dp_sink_demo_a2dp_connection_t * a2dp_conn = &a2dp_sink_demo_a2dp_connection;

    switch (packet[2]){
        case A2DP_SUBEVENT_SIGNALING_MEDIA_CODEC_OTHER_CONFIGURATION:
            printf("A2DP: Received non-SBC codec - not implemented\n");
            break;
        case A2DP_SUBEVENT_SIGNALING_MEDIA_CODEC_SBC_CONFIGURATION:{
            printf("A2DP: Received SBC codec configuration\n");
            a2dp_conn->sbc_configuration.reconfigure = a2dp_subevent_signaling_media_codec_sbc_configuration_get_reconfigure(packet);
            a2dp_conn->sbc_configuration.num_channels = a2dp_subevent_signaling_media_codec_sbc_configuration_get_num_channels(packet);
            a2dp_conn->sbc_configuration.sampling_frequency = a2dp_subevent_signaling_media_codec_sbc_configuration_get_sampling_frequency(packet);
            a2dp_conn->sbc_configuration.block_length = a2dp_subevent_signaling_media_codec_sbc_configuration_get_block_length(packet);
            a2dp_conn->sbc_configuration.subbands = a2dp_subevent_signaling_media_codec_sbc_configuration_get_subbands(packet);
            a2dp_conn->sbc_configuration.min_bitpool_value = a2dp_subevent_signaling_media_codec_sbc_configuration_get_min_bitpool_value(packet);
            a2dp_conn->sbc_configuration.max_bitpool_value = a2dp_subevent_signaling_media_codec_sbc_configuration_get_max_bitpool_value(packet);
            allocation_method = a2dp_subevent_signaling_media_codec_sbc_configuration_get_allocation_method(packet);
            a2dp_conn->sbc_configuration.allocation_method = (btstack_sbc_allocation_method_t)(allocation_method - 1);
            switch (a2dp_subevent_signaling_media_codec_sbc_configuration_get_channel_mode(packet)){
                case AVDTP_CHANNEL_MODE_JOINT_STEREO:
                    a2dp_conn->sbc_configuration.channel_mode = SBC_CHANNEL_MODE_JOINT_STEREO;
                    break;
                case AVDTP_CHANNEL_MODE_STEREO:
                    a2dp_conn->sbc_configuration.channel_mode = SBC_CHANNEL_MODE_STEREO;
                    break;
                case AVDTP_CHANNEL_MODE_DUAL_CHANNEL:
                    a2dp_conn->sbc_configuration.channel_mode = SBC_CHANNEL_MODE_DUAL_CHANNEL;
                    break;
                case AVDTP_CHANNEL_MODE_MONO:
                    a2dp_conn->sbc_configuration.channel_mode = SBC_CHANNEL_MODE_MONO;
                    break;
                default:
                    btstack_assert(false);
                    break;
            }
            dump_sbc_configuration(&a2dp_conn->sbc_configuration);
            break;
        }
        case A2DP_SUBEVENT_STREAM_ESTABLISHED:
            status = a2dp_subevent_stream_established_get_status(packet);
            if (status != ERROR_CODE_SUCCESS){
                printf("A2DP: Streaming connection failed, status 0x%02x\n", status);
                break;
            }
            a2dp_subevent_stream_established_get_bd_addr(packet, a2dp_conn->addr);
            a2dp_conn->a2dp_cid = a2dp_subevent_stream_established_get_a2dp_cid(packet);
            a2dp_conn->a2dp_local_seid = a2dp_subevent_stream_established_get_local_seid(packet);
            a2dp_conn->stream_state = STREAM_STATE_OPEN;
            printf("A2DP: Stream established, addr %s\n", bd_addr_to_str(a2dp_conn->addr));
            memcpy(device_addr, a2dp_conn->addr, 6);
            break;

        case A2DP_SUBEVENT_STREAM_STARTED:
            printf("A2DP: Stream started\n");
            a2dp_conn->stream_state = STREAM_STATE_PLAYING;
            if (a2dp_conn->sbc_configuration.reconfigure){
                media_processing_close();
            }
            media_processing_init(&a2dp_conn->sbc_configuration);
            ipc_send_event_stream_started();
            break;

        case A2DP_SUBEVENT_STREAM_SUSPENDED:
            printf("A2DP: Stream paused\n");
            a2dp_conn->stream_state = STREAM_STATE_PAUSED;
            media_processing_pause();
            ipc_send_event_stream_stopped();
            break;

        case A2DP_SUBEVENT_STREAM_RELEASED:
            printf("A2DP: Stream released\n");
            a2dp_conn->stream_state = STREAM_STATE_CLOSED;
            media_processing_close();
            ipc_send_event_stream_stopped();
            break;

        case A2DP_SUBEVENT_SIGNALING_CONNECTION_RELEASED:
            printf("A2DP: Signaling connection released\n");
            a2dp_conn->a2dp_cid = 0;
            media_processing_close();
            break;

        default:
            break;
    }
}


// ============================================================================
// Setup
// ============================================================================

static int setup_demo(void){
    l2cap_init();
    sdp_init();
#ifdef ENABLE_BLE
    sm_init();
#endif
#ifdef ENABLE_AVRCP_COVER_ART
    goep_client_init();
    avrcp_cover_art_client_init();
#endif

    a2dp_sink_init();
    avrcp_init();
    avrcp_controller_init();
    avrcp_target_init();

    a2dp_sink_register_packet_handler(&a2dp_sink_packet_handler);
    a2dp_sink_register_media_handler(&handle_l2cap_media_data_packet);
    a2dp_sink_demo_stream_endpoint_t * stream_endpoint = &a2dp_sink_demo_stream_endpoint;
    avdtp_stream_endpoint_t * local_stream_endpoint = a2dp_sink_create_stream_endpoint(AVDTP_AUDIO,
                                                                                       AVDTP_CODEC_SBC, media_sbc_codec_capabilities, sizeof(media_sbc_codec_capabilities),
                                                                                       stream_endpoint->media_sbc_codec_configuration, sizeof(stream_endpoint->media_sbc_codec_configuration));
    btstack_assert(local_stream_endpoint != NULL);
    stream_endpoint->a2dp_local_seid = avdtp_local_seid(local_stream_endpoint);

    avrcp_register_packet_handler(&avrcp_packet_handler);
    avrcp_controller_register_packet_handler(&avrcp_controller_packet_handler);
    avrcp_target_register_packet_handler(&avrcp_target_packet_handler);

    // SDP records
    memset(sdp_avdtp_sink_service_buffer, 0, sizeof(sdp_avdtp_sink_service_buffer));
    a2dp_sink_create_sdp_record(sdp_avdtp_sink_service_buffer, sdp_create_service_record_handle(),
                                AVDTP_SINK_FEATURE_MASK_HEADPHONE, NULL, NULL);
    btstack_assert(de_get_len(sdp_avdtp_sink_service_buffer) <= sizeof(sdp_avdtp_sink_service_buffer));
    sdp_register_service(sdp_avdtp_sink_service_buffer);

    memset(sdp_avrcp_controller_service_buffer, 0, sizeof(sdp_avrcp_controller_service_buffer));
    uint16_t controller_supported_features = 1 << AVRCP_CONTROLLER_SUPPORTED_FEATURE_CATEGORY_PLAYER_OR_RECORDER;
#ifdef ENABLE_AVRCP_COVER_ART
    controller_supported_features |= 1 << AVRCP_CONTROLLER_SUPPORTED_FEATURE_COVER_ART_GET_LINKED_THUMBNAIL;
#endif
    avrcp_controller_create_sdp_record(sdp_avrcp_controller_service_buffer, sdp_create_service_record_handle(),
                                       controller_supported_features, NULL, NULL);
    btstack_assert(de_get_len(sdp_avrcp_controller_service_buffer) <= sizeof(sdp_avrcp_controller_service_buffer));
    sdp_register_service(sdp_avrcp_controller_service_buffer);

    memset(sdp_avrcp_target_service_buffer, 0, sizeof(sdp_avrcp_target_service_buffer));
    uint16_t target_supported_features = 1 << AVRCP_TARGET_SUPPORTED_FEATURE_CATEGORY_MONITOR_OR_AMPLIFIER;
    avrcp_target_create_sdp_record(sdp_avrcp_target_service_buffer,
                                   sdp_create_service_record_handle(), target_supported_features, NULL, NULL);
    btstack_assert(de_get_len(sdp_avrcp_target_service_buffer) <= sizeof(sdp_avrcp_target_service_buffer));
    sdp_register_service(sdp_avrcp_target_service_buffer);

    memset(device_id_sdp_service_buffer, 0, sizeof(device_id_sdp_service_buffer));
    device_id_create_sdp_record(device_id_sdp_service_buffer,
                                sdp_create_service_record_handle(), DEVICE_ID_VENDOR_ID_SOURCE_BLUETOOTH, BLUETOOTH_COMPANY_ID_BLUEKITCHEN_GMBH, 1, 1);
    btstack_assert(de_get_len(device_id_sdp_service_buffer) <= sizeof(device_id_sdp_service_buffer));
    sdp_register_service(device_id_sdp_service_buffer);

    // GAP configuration
    gap_set_local_name("Bluetooth Media Bridge 00:00:00:00:00:00");
    gap_discoverable_control(1);
    gap_set_class_of_device(0x200404);
    gap_set_default_link_policy_settings(LM_LINK_POLICY_ENABLE_ROLE_SWITCH | LM_LINK_POLICY_ENABLE_SNIFF_MODE);
    gap_set_allow_role_switch(true);

    hci_event_callback_registration.callback = &hci_packet_handler;
    hci_add_event_handler(&hci_event_callback_registration);

    // Initialize metadata
    memset(current_title, 0, sizeof(current_title));
    memset(current_artist, 0, sizeof(current_artist));
    memset(current_album, 0, sizeof(current_album));
    memset(current_genre, 0, sizeof(current_genre));
    memset(current_image_handle, 0, sizeof(current_image_handle));
    snprintf(current_playback_status, sizeof(current_playback_status), "stopped");
    memset(prev_title, 0, sizeof(prev_title));
    memset(prev_artist, 0, sizeof(prev_artist));
    memset(prev_image_handle, 0, sizeof(prev_image_handle));
    metadata_complete = false;

    // Audio playback info
    if (!btstack_audio_sink_get_instance()){
        printf("No audio playback.\n");
    } else {
        printf("Audio playback supported.\n");
    }

    return 0;
}


// ============================================================================
// Stdin Handler (for debugging, kept from a2dp_sink_demo.c)
// ============================================================================

#ifdef HAVE_BTSTACK_STDIN
static void stdin_process(char cmd){
    uint8_t status = ERROR_CODE_SUCCESS;
    a2dp_sink_demo_avrcp_connection_t * avrcp_connection = &a2dp_sink_demo_avrcp_connection;

    switch (cmd){
        case ' ':
            printf("\n--- Bluetooth Media Bridge Commands ---\n");
            printf("j - get now playing info\n");
            printf("k - play\n");
            printf("K - stop\n");
            printf("L - pause\n");
            printf("i - forward\n");
            printf("I - backward\n");
            printf("t/T - volume up/down\n");
#ifdef ENABLE_AVRCP_COVER_ART
            printf("@ - download cover art\n");
#endif
            printf("---\n");
            break;
        case 'j':
            status = avrcp_controller_get_now_playing_info(avrcp_connection->avrcp_cid);
            break;
        case 'k':
            status = avrcp_controller_play(avrcp_connection->avrcp_cid);
            break;
        case 'K':
            status = avrcp_controller_stop(avrcp_connection->avrcp_cid);
            break;
        case 'L':
            status = avrcp_controller_pause(avrcp_connection->avrcp_cid);
            break;
        case 'i':
            status = avrcp_controller_forward(avrcp_connection->avrcp_cid);
            break;
        case 'I':
            status = avrcp_controller_backward(avrcp_connection->avrcp_cid);
            break;
        case 't':
            volume_percentage = volume_percentage <= 90 ? volume_percentage + 10 : 100;
            {
                uint8_t volume = volume_percentage * 127 / 100;
                status = avrcp_target_volume_changed(avrcp_connection->avrcp_cid, volume);
                avrcp_volume_changed(volume);
            }
            break;
        case 'T':
            volume_percentage = volume_percentage >= 10 ? volume_percentage - 10 : 0;
            {
                uint8_t volume = volume_percentage * 127 / 100;
                status = avrcp_target_volume_changed(avrcp_connection->avrcp_cid, volume);
                avrcp_volume_changed(volume);
            }
            break;
#ifdef ENABLE_AVRCP_COVER_ART
        case '@':
            trigger_cover_art_download();
            break;
#endif
        case '\n':
        case '\r':
            break;
        default:
            break;
    }
    if (status != ERROR_CODE_SUCCESS){
        printf("Command failed, status 0x%02x\n", status);
    }
}
#endif


// ============================================================================
// Entry Point
// ============================================================================

int btstack_main(int argc, const char * argv[]);
int btstack_main(int argc, const char * argv[]){
    UNUSED(argc);
    (void)argv;

    // Initialize IPC server
    ipc_init();

    // Setup btstack demo
    setup_demo();

#ifdef HAVE_BTSTACK_STDIN
    btstack_stdin_setup(stdin_process);
#endif

    // Setup IPC polling timer (integrated with btstack run loop)
    btstack_run_loop_set_timer(&ipc_poll_timer, IPC_POLL_INTERVAL_MS);
    ipc_poll_timer.process = ipc_poll_timer_handler;
    btstack_run_loop_add_timer(&ipc_poll_timer);

    // Turn on Bluetooth
    printf("Starting Bluetooth Media Bridge ...\n");
    hci_power_control(HCI_POWER_ON);
    return 0;
}