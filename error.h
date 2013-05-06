#ifndef __ERROR_H_
#define __ERROR_H_
#include <stdarg.h>

static void __ERROR(const char* file, const int line, const char* fmt, ...) __attribute__ ((noreturn));
static void __ERROR(const char* file, const int line, const char* fmt, ...)
{
    va_list args;

    va_start(args, fmt);
    fprintf(stderr, "ERROR %s:%d ", file, line);
    vfprintf(stderr, fmt, args);
    va_end(args);
    abort();
}

#define ERROR(s, ...)  \
    __ERROR(__FILE__, __LINE__, s "\n", ##__VA_ARGS__)

#if 0
#define ERROR(s, ...)  { \
    fprintf(stderr, "ERROR %s:%d " s "\n", \
            __FILE__, __LINE__, ##__VA_ARGS__); \
    abort(); \
} while(0)
#endif

#define WARNING(s, ...)  { \
    fprintf(stderr, "WARNING %s:%d " s "\n", \
            __FILE__, __LINE__, ##__VA_ARGS__); \
} while(0)


#endif //__ERROR_H_
