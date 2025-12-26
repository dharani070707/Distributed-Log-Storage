// Broker.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <pthread.h>
#include <fcntl.h>

#define PORT 9092
#define NUM_PARTITIONS 3

#define PRODUCE    1
#define FETCH      2
#define JOIN_GROUP 3

#define MAX_PAYLOAD_SIZE (1024 * 1024)
#define MAX_FETCH_MSGS   5
#define MAX_FETCH_BYTES 1024

/* ---- ROOT DIRECTORIES (FIXED) ---- */
#define DATA_DIR   "../Data"
#define OFFSET_DIR "../offsets"

/* ---------- GLOBAL STATE ---------- */

long next_offset[NUM_PARTITIONS] = {0};
pthread_mutex_t partition_lock[NUM_PARTITIONS];

/* ---------- CONSUMER GROUP ---------- */

#define MAX_CONSUMERS 10

typedef struct {
    char consumer_id[32];
    int partition;
} GroupMember;

typedef struct {
    char group_id[32];
    int member_count;
    GroupMember members[MAX_CONSUMERS];
} ConsumerGroup;

ConsumerGroup group = {0};

/* ---------- HELPERS ---------- */

void build_paths(int p, char *log, char *idx) {
    snprintf(log, 256, "%s/partition-%d/orders.log", DATA_DIR, p);
    snprintf(idx, 256, "%s/partition-%d/orders.idx", DATA_DIR, p);
}

void ensure_dirs() {
    mkdir(DATA_DIR, 0777);
    for (int i = 0; i < NUM_PARTITIONS; i++) {
        char path[256];
        snprintf(path, sizeof(path), "%s/partition-%d", DATA_DIR, i);
        mkdir(path, 0777);
    }
}

void recover_offsets() {
    for (int p = 0; p < NUM_PARTITIONS; p++) {
        char log[256], idx[256];
        build_paths(p, log, idx);

        FILE *fp = fopen(log, "rb");
        if (!fp) continue;

        int len;
        long off;
        while (fread(&len, sizeof(int), 1, fp) == 1) {
            fread(&off, sizeof(long), 1, fp);
            fseek(fp, len, SEEK_CUR);
            next_offset[p] = off + 1;
        }
        fclose(fp);
    }
}

long find_position(const char *idx_path, long target) {
    FILE *fp = fopen(idx_path, "rb");
    if (!fp) return 0;

    long off, pos;
    while (fread(&off, sizeof(long), 1, fp) == 1) {
        fread(&pos, sizeof(long), 1, fp);
        if (off >= target) {
            fclose(fp);
            return pos;
        }
    }
    fclose(fp);
    return 0;
}

int assign_partition(int idx) {
    return idx % NUM_PARTITIONS;
}

/* ---------- CLIENT HANDLER ---------- */

void* client_handler(void *arg) {
    int client = *(int*)arg;
    free(arg);

    int total_len, type;
    if (read(client, &total_len, sizeof(int)) <= 0) {
        close(client);
        return NULL;
    }
    read(client, &type, sizeof(int));

    /* ===== PRODUCE ===== */
    if (type == PRODUCE) {
        int key, payload_len;
        read(client, &key, sizeof(int));
        read(client, &payload_len, sizeof(int));

        if (payload_len <= 0 || payload_len > MAX_PAYLOAD_SIZE) {
            close(client);
            return NULL;
        }

        char *payload = malloc(payload_len);
        read(client, payload, payload_len);

        int p = key % NUM_PARTITIONS;
        pthread_mutex_lock(&partition_lock[p]);

        char log[256], idx[256];
        build_paths(p, log, idx);

        FILE *lf = fopen(log, "ab");
        FILE *ifp = fopen(idx, "ab");

        long pos = ftell(lf);
        long off = next_offset[p];

        fwrite(&payload_len, sizeof(int), 1, lf);
        fwrite(&off, sizeof(long), 1, lf);
        fwrite(payload, payload_len, 1, lf);
        fflush(lf);
        fsync(fileno(lf));

        fwrite(&off, sizeof(long), 1, ifp);
        fwrite(&pos, sizeof(long), 1, ifp);
        fflush(ifp);
        fsync(fileno(ifp));

        fclose(lf);
        fclose(ifp);

        next_offset[p]++;
        pthread_mutex_unlock(&partition_lock[p]);

        write(client, &p, sizeof(int));
        write(client, &off, sizeof(long));

        printf("[PRODUCE] key=%d -> partition=%d offset=%ld\n", key, p, off);
        free(payload);
    }

    /* ===== FETCH ===== */
    else if (type == FETCH) {
        int p;
        long fetch_offset;
        read(client, &p, sizeof(int));
        read(client, &fetch_offset, sizeof(long));

        if (p < 0 || p >= NUM_PARTITIONS || fetch_offset < 0) {
            close(client);
            return NULL;
        }

        char log[256], idx[256];
        build_paths(p, log, idx);

        long start = find_position(idx, fetch_offset);
        FILE *lf = fopen(log, "rb");
        if (!lf) {
            int zero = 0;
            write(client, &zero, sizeof(int));
            close(client);
            return NULL;
        }

        fseek(lf, start, SEEK_SET);

        int count = 0, bytes = 0;
        long offs[100];
        int lens[100];
        char *msgs[100];

        int len;
        long off;
        while (fread(&len, sizeof(int), 1, lf) == 1) {
            fread(&off, sizeof(long), 1, lf);

            if (off < fetch_offset) {
                fseek(lf, len, SEEK_CUR);
                continue;
            }
            if (count >= MAX_FETCH_MSGS || bytes + len > MAX_FETCH_BYTES)
                break;

            char *buf = malloc(len);
            fread(buf, len, 1, lf);

            offs[count] = off;
            lens[count] = len;
            msgs[count] = buf;

            bytes += len;
            count++;
        }

        write(client, &count, sizeof(int));
        for (int i = 0; i < count; i++) {
            write(client, &offs[i], sizeof(long));
            write(client, &lens[i], sizeof(int));
            write(client, msgs[i], lens[i]);
            free(msgs[i]);
        }
        fclose(lf);
    }

    /* ===== JOIN GROUP ===== */
    else if (type == JOIN_GROUP) {
        char group_id[32], consumer_id[32];
        read(client, group_id, sizeof(group_id));
        read(client, consumer_id, sizeof(consumer_id));

        if (group.member_count == 0)
            strcpy(group.group_id, group_id);

        int idx = group.member_count;
        strcpy(group.members[idx].consumer_id, consumer_id);
        group.members[idx].partition = assign_partition(idx);
        group.member_count++;

        int p = group.members[idx].partition;
        write(client, &p, sizeof(int));

        printf("[GROUP] %s -> %s assigned partition %d\n",
               group_id, consumer_id, p);
    }

    close(client);
    return NULL;
}

/* ---------- MAIN ---------- */

int main() {
    ensure_dirs();
    recover_offsets();

    for (int i = 0; i < NUM_PARTITIONS; i++)
        pthread_mutex_init(&partition_lock[i], NULL);

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    bind(server_fd, (struct sockaddr*)&addr, sizeof(addr));
    listen(server_fd, 20);

    printf("Broker running on port %d\n", PORT);

    while (1) {
        int *client = malloc(sizeof(int));
        *client = accept(server_fd, NULL, NULL);

        pthread_t tid;
        pthread_create(&tid, NULL, client_handler, client);
        pthread_detach(tid);
    }
}
