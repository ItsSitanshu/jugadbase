#ifndef DATETIME_H
#define DATETIME_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#define JUGADBASE_EPOCH_YEAR 2000
#define JUGADBASE_DATE_EPOCH 730120

#define __cmp_dt(x) if (a->x != b->x) return a->x - b->x

typedef int32_t Date;
typedef int64_t TimeStored;

typedef struct {
  int64_t timestamp;
} Timestamp;

typedef struct {
  int64_t timestamp;
  int32_t time_zone_offset;
} Timestamp_TZ;

typedef struct {
  int hour, minute, second;
} TimeOnly;

typedef struct {
  TimeStored time; 
  int32_t time_zone_offset;
} Time_TZ;

typedef struct {
  int32_t months;
  int32_t days;
  int64_t micros;
} Interval;

typedef struct {
  int year, month, day;
  int hour, minute, second;
} __dt;

typedef struct {
  int year, month, day;
  int hour, minute, second;
} DateTime;

typedef struct {
  int year, month, day;
  int hour, minute, second;
  int32_t time_zone_offset;
} DateTime_TZ;

bool parse_datetime(const char* str, __dt* out);
bool is_valid_date(int y, int m, int d);
int compare_datetime(const __dt* a, const __dt* b);
time_t to_epoch(const __dt* dt);

Date encode_date(int y, int m, int d);
void decode_date(Date encoded, int* y, int* m, int* d);

TimeStored encode_time(int h, int m, int s);
void decode_time(TimeStored encoded, int* h, int* m, int* s);

Timestamp encode_timestamp(const __dt* dt);
Timestamp_TZ encode_timestamp_TZ(const __dt* dt, int32_t tz_offset);
void decode_timestamp(Timestamp encoded, __dt* out);
void decode_timestamp_TZ(Timestamp_TZ encoded, __dt* out);

TimeStored encode_time_only(int h, int m, int s);
Time_TZ encode_time_TZ(int h, int m, int s, int32_t tz_offset);
void decode_time_only(TimeStored encoded, int* h, int* m, int* s);
void decode_time_TZ(Time_TZ tt, int* h, int* m, int* s, int32_t* tz_offset);

DateTime create_datetime(int y, int m, int d, int h, int mi, int s);
DateTime_TZ create_datetime_TZ(int y, int m, int d, int h, int mi, int s, int32_t tz_offset);

bool is_valid_datetime(DateTime dt);
bool is_valid_datetime_TZ(DateTime_TZ dt);

DateTime timestamp_to_datetime(Timestamp ts);
Timestamp datetime_to_timestamp(DateTime dt);
DateTime_TZ timestamp_TZ_to_datetime_TZ(Timestamp_TZ ts);
Timestamp_TZ datetime_TZ_to_timestamp_TZ(DateTime_TZ dt);

bool parse_to_datetime(const char* str, DateTime* out); 
bool parse_to_datetime_TZ(const char* str, DateTime_TZ* out);
char* datetime_TZ_to_string(DateTime_TZ dt, char* buffer, size_t buffer_size);

int compare_datetime_objs(const DateTime* a, const DateTime* b);
int compare_datetime_TZ(const DateTime_TZ* a, const DateTime_TZ* b);

DateTime add_interval_to_datetime(DateTime dt, Interval interval);
DateTime_TZ add_interval_to_datetime_TZ(DateTime_TZ dt, Interval interval);
Interval datetime_diff(DateTime start, DateTime end);

DateTime_TZ datetime_to_TZ(DateTime dt, int32_t tz_offset);
DateTime datetime_TZ_to_UTC(DateTime_TZ dt);
DateTime_TZ datetime_TZ_convert(DateTime_TZ dt, int32_t new_tz_offset);

int get_timezone_offset();

char* date_to_string(Date date);
char* time_to_string(TimeStored time);
char* timestamp_to_string(Timestamp time);
char* datetime_to_string(DateTime dt);
char* interval_to_string(Interval interval);
char* time_tz_to_string(Time_TZ tt);
char* timestamp_tz_to_string(Timestamp_TZ encoded);

#endif // DATETIME_H