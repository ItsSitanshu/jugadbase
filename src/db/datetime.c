#include "datetime.h"

#include "../utils/log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <math.h>

#define SECONDS_PER_MINUTE 60
#define SECONDS_PER_HOUR 3600
#define SECONDS_PER_DAY 86400
#define DAYS_PER_MONTH 30  // Approximation
#define DAYS_PER_YEAR 365
#define MONTHS_PER_YEAR 12
#define MICROS_PER_SECOND 1000000

static bool is_leap_year(int year);
static int days_in_month(int year, int month);
static int day_of_year(int year, int month, int day);
static void year_day_to_month_day(int year, int day_of_year, int* month, int* day);
static int days_between_years(int y1, int y2);

bool is_valid_date(int y, int m, int d) {
  if (m < 1 || m > 12)
    return false;
  
  return d >= 1 && d <= days_in_month(y, m);
}

bool is_valid_time(int h, int m, int s) {
  return h >= 0 && h < 24 && m >= 0 && m < 60 && s >= 0 && s < 60;
}

bool is_valid_datetime(DateTime dt) {
  return is_valid_date(dt.year, dt.month, dt.day) && 
         is_valid_time(dt.hour, dt.minute, dt.second);
}

bool is_valid_datetime_TZ(DateTime_TZ dt) {
  // Time zone offset is in minutes, valid range is typically -720 to 840 minutes
  // (-12 hours to +14 hours)
  return is_valid_datetime((DateTime){dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second}) &&
         dt.time_zone_offset >= -720 && dt.time_zone_offset <= 840;
}

Date encode_date(int y, int m, int d) {
  if (!is_valid_date(y, m, d))
    return -1;
  
  Date days = JUGADBASE_DATE_EPOCH;
  
  days += days_between_years(JUGADBASE_EPOCH_YEAR, y);
  
  days += day_of_year(y, m, d) - 1; // -1 because day_of_year is 1-based
  
  return days;
}

void decode_date(Date encoded, int* y, int* m, int* d) {
  int year = JUGADBASE_EPOCH_YEAR;
  int days_remaining = encoded - JUGADBASE_DATE_EPOCH;
  
  while (true) {
    int days_in_year = is_leap_year(year) ? 366 : 365;
    if (days_remaining < days_in_year)
      break;
    days_remaining -= days_in_year;
    year++;
  }
  
  year_day_to_month_day(year, days_remaining + 1, m, d); // +1 because year_day_to_month_day is 0-based
  *y = year;
}

TimeStored encode_time(int h, int m, int s) {
  if (!is_valid_time(h, m, s))
    return -1;
      
  return ((int64_t)h * 3600 + (int64_t)m * 60 + s) * MICROS_PER_SECOND;
}

void decode_time(TimeStored encoded, int* h, int* m, int* s) {
  int64_t seconds = encoded / MICROS_PER_SECOND;
  
  *h = seconds / 3600;
  seconds %= 3600;
  
  *m = seconds / 60;
  *s = seconds % 60;
}

TimeStored encode_time_only(int h, int m, int s) {
  return encode_time(h, m, s);
}

void decode_time_only(TimeStored encoded, int* h, int* m, int* s) {
  decode_time(encoded, h, m, s);
}

Time_TZ encode_time_TZ(int h, int m, int s, int32_t tz_offset) {
  Time_TZ result;
  result.time = encode_time_only(h, m, s);
  result.time_zone_offset = tz_offset;
  return result;
}

void decode_time_TZ(Time_TZ tt, int* h, int* m, int* s, int32_t* tz_offset) {
  decode_time_only(tt.time, h, m, s);
  *tz_offset = tt.time_zone_offset;
}

Timestamp encode_timestamp(const __dt* dt) {
  Timestamp result;
  
  Date days = encode_date(dt->year, dt->month, dt->day);
  
  TimeStored time = encode_time(dt->hour, dt->minute, dt->second);
  
  result.timestamp = (int64_t)days * SECONDS_PER_DAY * MICROS_PER_SECOND + time;
  
  return result;
}

void decode_timestamp(Timestamp encoded, __dt* out) {
  int64_t total_micros = encoded.timestamp;
  int64_t days_micros = SECONDS_PER_DAY * MICROS_PER_SECOND;
  
  Date days = total_micros / days_micros;
  TimeStored time_micros = total_micros % days_micros;
  
  decode_date(days, &out->year, &out->month, &out->day);
  
  decode_time(time_micros, &out->hour, &out->minute, &out->second);
}

Timestamp_TZ encode_timestamp_TZ(const __dt* dt, int32_t tz_offset) {
  Timestamp_TZ result;
  result.timestamp = encode_timestamp(dt).timestamp;
  result.time_zone_offset = tz_offset;
  return result;
}

void decode_timestamp_TZ(Timestamp_TZ encoded, __dt* out) {
  decode_timestamp((Timestamp){encoded.timestamp}, out);
}

DateTime create_datetime(int y, int m, int d, int h, int mi, int s) {
  DateTime dt;
  dt.year = y;
  dt.month = m;
  dt.day = d;
  dt.hour = h;
  dt.minute = mi;
  dt.second = s;
  return dt;
}

DateTime_TZ create_datetime_TZ(int y, int m, int d, int h, int mi, int s, int32_t tz_offset) {
  DateTime_TZ dt;
  dt.year = y;
  dt.month = m;
  dt.day = d;
  dt.hour = h;
  dt.minute = mi;
  dt.second = s;
  dt.time_zone_offset = tz_offset;
  return dt;
}

DateTime timestamp_to_datetime(Timestamp ts) {
  __dt temp;
  decode_timestamp(ts, &temp);
  return create_datetime(temp.year, temp.month, temp.day, 
                       temp.hour, temp.minute, temp.second);
}

Timestamp datetime_to_timestamp(DateTime dt) {
  __dt temp;
  temp.year = dt.year;
  temp.month = dt.month;
  temp.day = dt.day;
  temp.hour = dt.hour;
  temp.minute = dt.minute;
  temp.second = dt.second;
  return encode_timestamp(&temp);
}

DateTime_TZ timestamp_TZ_to_datetime_TZ(Timestamp_TZ ts) {
  __dt temp;
  decode_timestamp((Timestamp){ts.timestamp}, &temp);
  return create_datetime_TZ(temp.year, temp.month, temp.day, 
                          temp.hour, temp.minute, temp.second,
                          ts.time_zone_offset);
}

Timestamp_TZ datetime_TZ_to_timestamp_TZ(DateTime_TZ dt) {
  __dt temp;
  temp.year = dt.year;
  temp.month = dt.month;
  temp.day = dt.day;
  temp.hour = dt.hour;
  temp.minute = dt.minute;
  temp.second = dt.second;
  
  Timestamp_TZ result;
  result.timestamp = encode_timestamp(&temp).timestamp;
  result.time_zone_offset = dt.time_zone_offset;
  return result;
}

bool parse_datetime(const char* str, __dt* out) {
  // - YYYY-MM-DD HH:MM:SS
  // - YYYY-MM-DD
  // - HH:MM:SS
  
  int values[6] = {0}; // year, month, day, hour, minute, second
  int fields_read = 0;
  // YYYY-MM-DD HH:MM:SS
  fields_read = sscanf(str, "%d-%d-%d %d:%d:%d", 
                       &values[0], &values[1], &values[2], 
                       &values[3], &values[4], &values[5]);
                       

  if (fields_read == 6) {
    out->year = values[0];
    out->month = values[1];
    out->day = values[2];
    out->hour = values[3];
    out->minute = values[4];
    out->second = values[5];
    LOG_DEBUG("%d %d %d %d %d %d\n", values[0], values[1], values[2], values[3], values[4], values[5]);

    LOG_DEBUG("=> %s, %d", str, fields_read);
    LOG_DEBUG("=> %d && %d", is_valid_date(out->year, out->month, out->day), is_valid_time(out->hour, out->minute, out->second));
    return is_valid_date(out->year, out->month, out->day) && 
           is_valid_time(out->hour, out->minute, out->second);
  }
  
  // YYYY-MM-DD
  fields_read = sscanf(str, "%d-%d-%d", &values[0], &values[1], &values[2]);
  if (fields_read == 3) {
    out->year = values[0];
    out->month = values[1];
    out->day = values[2];
    out->hour = 0;
    out->minute = 0;
    out->second = 0;
    return is_valid_date(out->year, out->month, out->day);
  }
  
  // HH:MM:SS
  fields_read = sscanf(str, "%d:%d:%d", &values[3], &values[4], &values[5]);
  if (fields_read == 3) {
    out->year = JUGADBASE_EPOCH_YEAR;
    out->month = 1;
    out->day = 1;
    out->hour = values[3];
    out->minute = values[4];
    out->second = values[5];
    return is_valid_time(out->hour, out->minute, out->second);
  }
  
  return false;
}

bool parse_to_datetime(const char* str, DateTime* out) {
  __dt temp;
  bool result = parse_datetime(str, &temp);
  if (result) {
    out->year = temp.year;
    out->month = temp.month;
    out->day = temp.day;
    out->hour = temp.hour;
    out->minute = temp.minute;
    out->second = temp.second;
  }
  return result;
}

bool parse_to_datetime_TZ(const char* str, DateTime_TZ* out) {
  // Format: YYYY-MM-DD HH:MM:SS±HH:MM or YYYY-MM-DD HH:MM:SS±HHMM
  __dt dt_part;
  int tz_hours = 0, tz_minutes = 0;
  char tz_sign = '+';
  char* tz_part;
  
  tz_part = strpbrk(str, "+-");
  if (!tz_part) {
    if (!parse_datetime(str, &dt_part))
      return false;
    
    out->year = dt_part.year;
    out->month = dt_part.month;
    out->day = dt_part.day;
    out->hour = dt_part.hour;
    out->minute = dt_part.minute;
    out->second = dt_part.second;
    out->time_zone_offset = 0; // Default to UTC
    return true;
  }
  
  size_t dt_len = tz_part - str;
  char* dt_str = (char*)malloc(dt_len + 1);
  if (!dt_str)
    return false;
      
  strncpy(dt_str, str, dt_len);
  dt_str[dt_len] = '\0';
  
  bool parse_success = parse_datetime(dt_str, &dt_part);
  free(dt_str);
  
  if (!parse_success)
    return false;
  
  tz_sign = *tz_part;
  tz_part++; 
  
  if (sscanf(tz_part, "%d:%d", &tz_hours, &tz_minutes) != 2) {
    if (sscanf(tz_part, "%2d%2d", &tz_hours, &tz_minutes) != 2) {
      return false;
    }
  }
  
  int32_t offset = tz_hours * 60 + tz_minutes;
  if (tz_sign == '-')
    offset = -offset;
  
  out->year = dt_part.year;
  out->month = dt_part.month;
  out->day = dt_part.day;
  out->hour = dt_part.hour;
  out->minute = dt_part.minute;
  out->second = dt_part.second;
  out->time_zone_offset = offset;
  
  return true;
}

char* datetime_to_string(DateTime dt, char* buffer, size_t buffer_size) {
  snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d",
           dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second);
  return buffer;
}

char* datetime_TZ_to_string(DateTime_TZ dt, char* buffer, size_t buffer_size) {
  int abs_offset = abs(dt.time_zone_offset);
  int tz_hours = abs_offset / 60;
  int tz_minutes = abs_offset % 60;
  
  snprintf(buffer, buffer_size, "%04d-%02d-%02d %02d:%02d:%02d%c%02d:%02d",
           dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second,
           (dt.time_zone_offset >= 0) ? '+' : '-',
           tz_hours, tz_minutes);
  return buffer;
}

int compare_datetime(const __dt* a, const __dt* b) {
  __cmp_dt(year);
  __cmp_dt(month);
  __cmp_dt(day);
  __cmp_dt(hour);
  __cmp_dt(minute);
  __cmp_dt(second);
  return 0;
}

int compare_datetime_objs(const DateTime* a, const DateTime* b) {
  if (a->year != b->year) return a->year - b->year;
  if (a->month != b->month) return a->month - b->month;
  if (a->day != b->day) return a->day - b->day;
  if (a->hour != b->hour) return a->hour - b->hour;
  if (a->minute != b->minute) return a->minute - b->minute;
  return a->second - b->second;
}

int compare_datetime_TZ(const DateTime_TZ* a, const DateTime_TZ* b) {
  DateTime_TZ a_utc = *a;
  DateTime_TZ b_utc = *b;
  
  int64_t a_minutes = (int64_t)a->hour * 60 + a->minute - a->time_zone_offset;
  int64_t b_minutes = (int64_t)b->hour * 60 + b->minute - b->time_zone_offset;
  
  while (a_minutes < 0) {
    a_minutes += 24 * 60;
    a_utc.day--;
    if (a_utc.day < 1) {
      a_utc.month--;
      if (a_utc.month < 1) {
        a_utc.month = 12;
        a_utc.year--;
      }
      a_utc.day = days_in_month(a_utc.year, a_utc.month);
    }
  }
  
  while (b_minutes < 0) {
    b_minutes += 24 * 60;
    b_utc.day--;
    if (b_utc.day < 1) {
      b_utc.month--;
      if (b_utc.month < 1) {
        b_utc.month = 12;
        b_utc.year--;
      }
      b_utc.day = days_in_month(b_utc.year, b_utc.month);
    }
  }
  
  while (a_minutes >= 24 * 60) {
    a_minutes -= 24 * 60;
    a_utc.day++;
    if (a_utc.day > days_in_month(a_utc.year, a_utc.month)) {
      a_utc.day = 1;
      a_utc.month++;
      if (a_utc.month > 12) {
        a_utc.month = 1;
        a_utc.year++;
      }
    }
  }
  
  while (b_minutes >= 24 * 60) {
    b_minutes -= 24 * 60;
    b_utc.day++;
    if (b_utc.day > days_in_month(b_utc.year, b_utc.month)) {
      b_utc.day = 1;
      b_utc.month++;
      if (b_utc.month > 12) {
        b_utc.month = 1;
        b_utc.year++;
      }
    }
  }
  
  a_utc.hour = a_minutes / 60;
  a_utc.minute = a_minutes % 60;
  b_utc.hour = b_minutes / 60;
  b_utc.minute = b_minutes % 60;
  
  if (a_utc.year != b_utc.year) return a_utc.year - b_utc.year;
  if (a_utc.month != b_utc.month) return a_utc.month - b_utc.month;
  if (a_utc.day != b_utc.day) return a_utc.day - b_utc.day;
  if (a_utc.hour != b_utc.hour) return a_utc.hour - b_utc.hour;
  if (a_utc.minute != b_utc.minute) return a_utc.minute - b_utc.minute;
  return a_utc.second - b_utc.second;
}

time_t to_epoch(const __dt* dt) {
  struct tm tm_time;
  memset(&tm_time, 0, sizeof(struct tm));
  
  tm_time.tm_year = dt->year - 1900;  // Years since 1900
  tm_time.tm_mon = dt->month - 1;     // Months are 0-11
  tm_time.tm_mday = dt->day;
  tm_time.tm_hour = dt->hour;
  tm_time.tm_min = dt->minute;
  tm_time.tm_sec = dt->second;
  
  return mktime(&tm_time);
}

DateTime add_interval_to_datetime(DateTime dt, Interval interval) {
  dt.year += interval.months / 12;
  dt.month += interval.months % 12;
  
  while (dt.month > 12) {
    dt.month -= 12;
    dt.year++;
  }
  while (dt.month < 1) {
    dt.month += 12;
    dt.year--;
  }
  
  int max_days = days_in_month(dt.year, dt.month);
  if (dt.day > max_days) {
    dt.day = max_days;
  }
  
  Date date_val = encode_date(dt.year, dt.month, dt.day);
  date_val += interval.days;
  decode_date(date_val, &dt.year, &dt.month, &dt.day);
  
  int64_t total_seconds = (int64_t)dt.hour * 3600 + dt.minute * 60 + dt.second;
  total_seconds += interval.micros / MICROS_PER_SECOND;

  while (total_seconds < 0) {
    total_seconds += SECONDS_PER_DAY;
    date_val--;
  }
  
  while (total_seconds >= SECONDS_PER_DAY) {
    total_seconds -= SECONDS_PER_DAY;
    date_val++;
  }
  
  if (date_val != encode_date(dt.year, dt.month, dt.day)) {
    decode_date(date_val, &dt.year, &dt.month, &dt.day);
  }
  
  dt.hour = total_seconds / 3600;
  total_seconds %= 3600;
  dt.minute = total_seconds / 60;
  dt.second = total_seconds % 60;
  
  return dt;
}

DateTime_TZ add_interval_to_datetime_TZ(DateTime_TZ dt, Interval interval) {
  DateTime temp = add_interval_to_datetime(
    (DateTime){dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second},
    interval
  );
  
  return (DateTime_TZ){
    temp.year, temp.month, temp.day,
    temp.hour, temp.minute, temp.second,
    dt.time_zone_offset
  };
}

Interval datetime_diff(DateTime start, DateTime end) {
  Interval result = {0};
  
  Timestamp start_ts = datetime_to_timestamp(start);
  Timestamp end_ts = datetime_to_timestamp(end);
  
  int64_t diff_micros = end_ts.timestamp - start_ts.timestamp;
  
  result.days = diff_micros / (SECONDS_PER_DAY * MICROS_PER_SECOND);
  diff_micros %= (SECONDS_PER_DAY * MICROS_PER_SECOND);
  
  result.micros = diff_micros;
  
  result.months = result.days / DAYS_PER_MONTH;
  result.days -= result.months * DAYS_PER_MONTH;
  
  return result;
}

DateTime_TZ datetime_to_TZ(DateTime dt, int32_t tz_offset) {
  return (DateTime_TZ){
    dt.year, dt.month, dt.day,
    dt.hour, dt.minute, dt.second,
    tz_offset
  };
}

DateTime datetime_TZ_to_UTC(DateTime_TZ dt) {
  DateTime result = {dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second};
  
  int64_t minutes = (int64_t)result.hour * 60 + result.minute - dt.time_zone_offset;
  
  while (minutes < 0) {
    minutes += 24 * 60;
    result.day--;
    if (result.day < 1) {
      result.month--;
      if (result.month < 1) {
        result.month = 12;
        result.year--;
      }
      result.day = days_in_month(result.year, result.month);
    }
  }
  
  while (minutes >= 24 * 60) {
    minutes -= 24 * 60;
    result.day++;
    if (result.day > days_in_month(result.year, result.month)) {
      result.day = 1;
      result.month++;
      if (result.month > 12) {
        result.month = 1;
        result.year++;
      }
    }
  }
  
  result.hour = minutes / 60;
  result.minute = minutes % 60;
  
  return result;
}

DateTime_TZ datetime_TZ_convert(DateTime_TZ dt, int32_t new_tz_offset) {
  DateTime utc = datetime_TZ_to_UTC(dt);
  
  DateTime_TZ result = datetime_to_TZ(utc, new_tz_offset);
  
  int64_t minutes = (int64_t)result.hour * 60 + result.minute + new_tz_offset;
  
  while (minutes < 0) {
    minutes += 24 * 60;
    result.day--;
    if (result.day < 1) {
      result.month--;
      if (result.month < 1) {
        result.month = 12;
        result.year--;
      }
      result.day = days_in_month(result.year, result.month);
    }
  }
  
  while (minutes >= 24 * 60) {
    minutes -= 24 * 60;
    result.day++;
    if (result.day > days_in_month(result.year, result.month)) {
      result.day = 1;
      result.month++;
      if (result.month > 12) {
        result.month = 1;
        result.year++;
      }
    }
  }

  result.minute = minutes % 60;
  
  return result;
}

static bool is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

static int days_in_month(int year, int month) {
  static const int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  
  if (month == 2 && is_leap_year(year))
    return 29;
      
  return days[month];
}

static int day_of_year(int year, int month, int day) {
  int doy = day;
  for (int m = 1; m < month; m++) {
    doy += days_in_month(year, m);
  }
  return doy;
}

static void year_day_to_month_day(int year, int day_of_year, int* month, int* day) {
  int m = 1;
  int days_in_current_month;
  
  while (day_of_year > 0) {
    days_in_current_month = days_in_month(year, m);
    if (day_of_year <= days_in_current_month)
      break;
          
    day_of_year -= days_in_current_month;
    m++;
  }
  
  *month = m;
  *day = day_of_year;
}

static int days_between_years(int y1, int y2) {
  int days = 0;
  
  if (y1 > y2) {
    int temp = y1;
    y1 = y2;
    y2 = temp;
    days = -days_between_years(y1, y2);
    return days;
  }
  
  for (int y = y1; y < y2; y++) {
    days += is_leap_year(y) ? 366 : 365;
  }
  
  return days;
}