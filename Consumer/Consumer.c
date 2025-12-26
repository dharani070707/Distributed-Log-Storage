// Consumer.c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/stat.h>

#define PORT 9092
#define JOIN_GROUP 3
#define FETCH 2

#define OFFSET_ROOT "../offsets"

long load_offset(const char *file) {
    FILE *fp = fopen(file, "r");
    if (!fp) return 0;
    long o;
    fscanf(fp, "%ld", &o);
    fclose(fp);
    return o;
}

void save_offset(const char *file, long o) {
    FILE *fp = fopen(file, "w");
    fprintf(fp, "%ld", o);
    fclose(fp);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        printf("Usage: %s <consumerId>\n", argv[0]);
        return 1;
    }

    char *consumer_id = argv[1];
    char group_id[32] = "orders-group";

    mkdir(OFFSET_ROOT, 0777);
    char group_dir[256];
    snprintf(group_dir, sizeof(group_dir), "%s/%s", OFFSET_ROOT, group_id);
    mkdir(group_dir, 0777);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in srv = {0};
    srv.sin_family = AF_INET;
    srv.sin_port = htons(PORT);
    srv.sin_addr.s_addr = inet_addr("127.0.0.1");

    connect(sock, (struct sockaddr*)&srv, sizeof(srv));

    int total = 4 + 32 + 32;
    int type = JOIN_GROUP;

    write(sock, &total, sizeof(int));
    write(sock, &type, sizeof(int));
    write(sock, group_id, sizeof(group_id));
    write(sock, consumer_id, 32);

    int partition;
    read(sock, &partition, sizeof(int));
    close(sock);

    printf("Consumer %s assigned partition %d\n", consumer_id, partition);

    char offset_file[256];
    int n = snprintf(
        offset_file,
        sizeof(offset_file),
        "%s/partition-%d.offset",
        group_dir,
        partition
    );

    if (n < 0 || n >= (int)sizeof(offset_file)) {
        fprintf(stderr, "Offset path too long\n");
        exit(1);
    }


    long offset = load_offset(offset_file);

    while (1) {
        sock = socket(AF_INET, SOCK_STREAM, 0);
        connect(sock, (struct sockaddr*)&srv, sizeof(srv));

        int fetch_len = 4 + 4 + 8;
        type = FETCH;

        write(sock, &fetch_len, sizeof(int));
        write(sock, &type, sizeof(int));
        write(sock, &partition, sizeof(int));
        write(sock, &offset, sizeof(long));

        int count;
        read(sock, &count, sizeof(int));

        for (int i = 0; i < count; i++) {
            long off;
            int len;
            read(sock, &off, sizeof(long));
            read(sock, &len, sizeof(int));

            char *buf = malloc(len + 1);
            read(sock, buf, len);
            buf[len] = '\0';

            printf("[Consumer %s] p%d offset %ld -> %s\n",
                   consumer_id, partition, off, buf);

            offset = off + 1;
            free(buf);
        }

        save_offset(offset_file, offset);
        close(sock);
        sleep(2);
    }
}
