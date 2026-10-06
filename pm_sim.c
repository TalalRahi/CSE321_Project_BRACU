#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>

#define MAX 64

const char* STATE_NAMES[] = {"EMPTY", "RUNNING", "ZOMBIE", "BLOCKED"};

typedef struct {
    int pid;
    int ppid;
    int status;
    int state; // 0=EMPTY, 1=RUNNING, 2=ZOMBIE, 3=BLOCKED
} PCB;

PCB table[MAX];
int next_pid = 1;
int running = 1;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
sem_t notify_monitor;

// -------------------- FORK --------------------
void pm_fork(int parent_pid) {
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX; i++) {
        if (table[i].state == 0) {
            table[i].pid = next_pid++;
            table[i].ppid = parent_pid;
            table[i].state = 1;
            table[i].status = -1;
            break;
        }
    }
    sem_post(&notify_monitor);
    pthread_mutex_unlock(&lock);
}

// -------------------- EXIT --------------------
void pm_exit(int pid, int exit_code) {
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX; i++) {
        if (table[i].pid == pid && table[i].state == 1) {
            table[i].state = 2; // ZOMBIE
            table[i].status = exit_code;
            break;
        }
    }
    sem_post(&notify_monitor);
    pthread_mutex_unlock(&lock);
}

// -------------------- WAIT --------------------
void pm_wait(int parent_pid, int child_pid) {
    pthread_mutex_lock(&lock);

    int found = 0;

    for (int i = 0; i < MAX; i++) {
        if (table[i].ppid == parent_pid) {
            found = 1;

            // If specific child OR any child (-1)
            if ((child_pid == -1 || table[i].pid == child_pid) && table[i].state == 2) {
                // Reap immediately
                table[i].state = 0;
                sem_post(&notify_monitor);
                pthread_mutex_unlock(&lock);
                return;
            }
        }
    }

    // No zombie found → block parent
    for (int i = 0; i < MAX; i++) {
        if (table[i].pid == parent_pid) {
            table[i].state = 3; // BLOCKED
        }
    }

    sem_post(&notify_monitor);
    pthread_mutex_unlock(&lock);

    sleep(1); // simulate wait

    pthread_mutex_lock(&lock);

    // Wake parent and reap child
    for (int i = 0; i < MAX; i++) {
        if (table[i].pid == parent_pid) {
            table[i].state = 1; // RUNNING
        }

        if (table[i].ppid == parent_pid && table[i].state == 2) {
            if (child_pid == -1 || table[i].pid == child_pid) {
                table[i].state = 0; // remove
                break;
            }
        }
    }

    sem_post(&notify_monitor);
    pthread_mutex_unlock(&lock);
}

// -------------------- KILL --------------------
void pm_kill(int pid) {
    pthread_mutex_lock(&lock);
    for (int i = 0; i < MAX; i++) {
        if (table[i].pid == pid && table[i].state == 1) {
            table[i].state = 2; // ZOMBIE
            table[i].status = -9; // killed
            break;
        }
    }
    sem_post(&notify_monitor);
    pthread_mutex_unlock(&lock);
}

// -------------------- MONITOR --------------------
void* monitor_func(void* arg) {
    FILE* f = fopen("snapshots.txt", "w");

    while (running) {
        sem_wait(&notify_monitor);

        fprintf(f, "--- Process Table Snapshot ---\n");
        fprintf(f, "%-5s %-5s %-10s %-12s\n", "PID", "PPID", "STATE", "EXIT_STATUS");

        pthread_mutex_lock(&lock);
        for (int i = 0; i < MAX; i++) {
            if (table[i].state != 0) {
                char status_str[10];
                if (table[i].status == -1) strcpy(status_str, "-");
                else sprintf(status_str, "%d", table[i].status);

                fprintf(f, "%-5d %-5d %-10s %-12s\n",
                        table[i].pid,
                        table[i].ppid,
                        STATE_NAMES[table[i].state],
                        status_str);
            }
        }
        pthread_mutex_unlock(&lock);

        fprintf(f, "\n");
        fflush(f);
    }

    fclose(f);
    return NULL;
}

// -------------------- WORKER --------------------
void* worker_func(void* filename) {
    FILE* f = fopen((char*)filename, "r");
    if (!f) return NULL;

    char line[100];

    while (fgets(line, sizeof(line), f)) {
        char cmd[20];
        int a = -1, b = -1;

        int count = sscanf(line, "%s %d %d", cmd, &a, &b);

        if (strcmp(cmd, "fork") == 0 && count >= 2) {
            pm_fork(a);
        }
        else if (strcmp(cmd, "exit") == 0 && count >= 3) {
            pm_exit(a, b);
        }
        else if (strcmp(cmd, "wait") == 0 && count >= 3) {
            pm_wait(a, b);
        }
        else if (strcmp(cmd, "kill") == 0 && count >= 2) {
            pm_kill(a);
        }
        else if (strcmp(cmd, "sleep") == 0 && count >= 2) {
            usleep(a * 1000); // ms → microseconds
        }
    }

    fclose(f);
    return NULL;
}

// -------------------- MAIN --------------------
int main(int argc, char* argv[]) {
    sem_init(&notify_monitor, 0, 0);

    // init process
    table[0].pid = next_pid++;
    table[0].ppid = 0;
    table[0].state = 1;
    table[0].status = -1;

    pthread_t mon;
    pthread_create(&mon, NULL, monitor_func, NULL);

    pthread_t workers[argc];

    for (int i = 1; i < argc; i++)
        pthread_create(&workers[i - 1], NULL, worker_func, argv[i]);

    for (int i = 1; i < argc; i++)
        pthread_join(workers[i - 1], NULL);

    sleep(1);

    running = 0;
    sem_post(&notify_monitor);

    pthread_join(mon, NULL);

    return 0;
}