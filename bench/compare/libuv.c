#include <stdio.h>
#include <stdlib.h>
#include <uv.h>

typedef struct {
    uv_tcp_t tcp;
    uv_write_t write;
    char buffer[4096];
} client;

static uv_loop_t loop;
static uv_tcp_t listener;
static client *clients;
static unsigned accepted;
static unsigned expected;

static void check(int status) {
    if (status < 0) {
        fprintf(stderr, "libuv: %s\n", uv_strerror(status));
        exit(1);
    }
}

static void allocate(uv_handle_t *handle, size_t suggested, uv_buf_t *buf) {
    (void)suggested;
    client *c = handle->data;
    *buf = uv_buf_init(c->buffer, sizeof(c->buffer));
}

static void read_done(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf);

static void write_done(uv_write_t *request, int status) {
    check(status);
    client *c = request->data;
    check(uv_read_start((uv_stream_t *)&c->tcp, allocate, read_done));
}

static void read_done(uv_stream_t *stream, ssize_t nread, const uv_buf_t *buf) {
    (void)buf;
    client *c = stream->data;
    if (nread == UV_EOF) {
        uv_close((uv_handle_t *)stream, NULL);
        return;
    }
    if (nread < 0) check((int)nread);
    if (nread == 0) return;
    // The buffer remains borrowed until write_done; do not read into it meanwhile.
    check(uv_read_stop(stream));
    uv_buf_t data = uv_buf_init(c->buffer, (unsigned)nread);
    check(uv_write(&c->write, stream, &data, 1, write_done));
}

static void connected(uv_stream_t *server, int status) {
    check(status);
    client *c = &clients[accepted];
    check(uv_tcp_init(&loop, &c->tcp));
    c->tcp.data = c;
    c->write.data = c;
    check(uv_accept(server, (uv_stream_t *)&c->tcp));
    check(uv_tcp_nodelay(&c->tcp, 1));
    check(uv_read_start((uv_stream_t *)&c->tcp, allocate, read_done));
    if (++accepted == expected) uv_close((uv_handle_t *)&listener, NULL);
}

int main(int argc, char **argv) {
    char *end = NULL;
    unsigned long count = argc == 2 ? strtoul(argv[1], &end, 10) : 0;
    if (!count || count > 10000 || !end || *end) {
        fprintf(stderr, "usage: echo-libuv <1..10000 connections>\n");
        return 1;
    }
    expected = (unsigned)count;
    clients = calloc(expected, sizeof(*clients));
    if (!clients) return 1;
    check(uv_loop_init(&loop));
    check(uv_tcp_init(&loop, &listener));
    struct sockaddr_in address;
    check(uv_ip4_addr("127.0.0.1", 0, &address));
    check(uv_tcp_bind(&listener, (const struct sockaddr *)&address, 0));
    check(uv_listen((uv_stream_t *)&listener, 1024, connected));
    int length = sizeof(address);
    check(uv_tcp_getsockname(&listener, (struct sockaddr *)&address, &length));
    printf("{\"port\":%u,\"backend\":\"epoll\",\"version\":\"%s\"}\n",
           ntohs(address.sin_port), uv_version_string());
    fflush(stdout);
    uv_run(&loop, UV_RUN_DEFAULT);
    check(uv_loop_close(&loop));
    free(clients);
    return 0;
}
