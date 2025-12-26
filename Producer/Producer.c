// Producer.c
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PORT 9092
#define PRODUCE 1
#define NUM_ORDERS 11   // 100 to 110 inclusive

int main() {
    int sock = socket(AF_INET, SOCK_STREAM, 0);

    struct sockaddr_in srv = {0};
    srv.sin_family = AF_INET;
    srv.sin_port = htons(PORT);
    srv.sin_addr.s_addr = inet_addr("127.0.0.1");

    if (connect(sock, (struct sockaddr*)&srv, sizeof(srv)) < 0) {
        perror("connect");
        return 1;
    }

    /* Generate orders 100 -> 110 */
    for (int i = 0; i < NUM_ORDERS; i++) {

        int sock = socket(AF_INET, SOCK_STREAM, 0);
        connect(sock, (struct sockaddr*)&srv, sizeof(srv));

        int orderId = 100 + i;
        int key = orderId;

        char payload[256];
        snprintf(payload, sizeof(payload),
                "orderId=%d product=P%d qty=%d price=%d",
                orderId,
                orderId % 5,
                (orderId % 3) + 1,
                100 + (orderId % 10) * 10);

        int payload_len = strlen(payload);
        int type = PRODUCE;
        int total_len = sizeof(int)*3 + payload_len;

        write(sock, &total_len, sizeof(int));
        write(sock, &type, sizeof(int));
        write(sock, &key, sizeof(int));
        write(sock, &payload_len, sizeof(int));
        write(sock, payload, payload_len);

        int partition;
        long offset;
        read(sock, &partition, sizeof(int));
        read(sock, &offset, sizeof(long));

        printf("Produced -> orderId=%d key=%d partition=%d offset=%ld\n",
            orderId, key, partition, offset);

        close(sock);
    }

    return 0;
}
