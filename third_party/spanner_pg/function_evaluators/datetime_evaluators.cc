//
// PostgreSQL is released under the PostgreSQL License, a liberal Open Source
// license, similar to the BSD or MIT licenses.
//
// PostgreSQL Database Management System
// (formerly known as Postgres, then as Postgres95)
//
// Portions Copyright © 1996-2020, The PostgreSQL Global Development Group
//
// Portions Copyright © 1994, The Regents of the University of California
//
// Portions Copyright 2023 Google LLC
//
// Permission to use, copy, modify, and distribute this software and its
// documentation for any purpose, without fee, and without a written agreement
// is hereby granted, provided that the above copyright notice and this
// paragraph and the following two paragraphs appear in all copies.
//
// IN NO EVENT SHALL THE UNIVERSITY OF CALIFORNIA BE LIABLE TO ANY PARTY FOR
// DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING
// LOST PROFITS, ARISING OUT OF THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION,
// EVEN IF THE UNIVERSITY OF CALIFORNIA HAS BEEN ADVISED OF THE POSSIBILITY OF
// SUCH DAMAGE.
//
// THE UNIVERSITY OF CALIFORNIA SPECIFICALLY DISCLAIMS ANY WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
// FITNESS FOR A PARTICULAR PURPOSE. THE SOFTWARE PROVIDED HEREUNDER IS ON AN
// "AS IS" BASIS, AND THE UNIVERSITY OF CALIFORNIA HAS NO OBLIGATIONS TO PROVIDE
// MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR MODIFICATIONS.
//------------------------------------------------------------------------------

#include "third_party/spanner_pg/interface/datetime_evaluators.h"

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <string>

#include "zetasql/public/functions/date_time_util.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/cord.h"
#include "absl/strings/match.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "third_party/spanner_pg/datatypes/common/pg_numeric_parse.h"
#include "third_party/spanner_pg/datatypes/extended/pg_numeric_type.h"
#include "third_party/spanner_pg/postgres_includes/all.h"  // IWYU pragma: keep
#include "third_party/spanner_pg/shims/error_shim.h"
#include "third_party/spanner_pg/shims/memory_context_pg_arena.h"  // IWYU pragma: keep
#include "third_party/spanner_pg/shims/stub_memory_reservation_manager.h"  // IWYU pragma: keep
#include "third_party/spanner_pg/shims/timezone_helper.h"  // IWYU pragma: keep
#include "third_party/spanner_pg/util/datetime_conversion.h"
#include "third_party/spanner_pg/util/integral_helpers.h"
#include "zetasql/base/status_macros.h"

namespace postgres_translator::function_evaluators {

using ::zetasql::functions::IsValidDate;
using ::postgres_translator::spangres::datatypes::common::NormalizePgNumeric;

inline constexpr char kPgTimestampOutOfRangeMessage[] =
    "date/time field value out of range";

inline constexpr char kIntervalOutOfRangeMessage[] = "interval out of range";

void CleanupPostgresDateTimeCache() { CleanupDateTimeCache(); }

static bool is_out_of_range_error(absl::Status status) {
  return status.code() == absl::StatusCode::kInvalidArgument &&
         absl::StrContains(status.message(), kPgTimestampOutOfRangeMessage);
}

absl::StatusOr<int32_t> DateFromDatumOr(absl::StatusOr<Datum>& date_or_error) {
  // Remaps the pg out of range error message
  if (is_out_of_range_error(date_or_error.status())) {
    return kInvalidDate;
  }
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_date, date_or_error);
  ZETASQL_ASSIGN_OR_RETURN(int32_t gsql_date,
                   SafePgDateOffsetToGsqlDateOffset(pg_date));
  if (!IsValidDate(gsql_date)) {
    return kInvalidDate;
  }

  return gsql_date;
}

absl::StatusOr<absl::Time> TimestampFromDatumOr(
    absl::StatusOr<Datum>& timestamp_or_error) {
  // Remaps the pg out of range error message
  if (is_out_of_range_error(timestamp_or_error.status())) {
    return kInvalidTimestamp;
  }
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_timestamptz, timestamp_or_error);

  absl::Time gsql_timestamp =
      PgTimestamptzToAbslTime(DatumGetTimestampTz(pg_timestamptz));

  if (!zetasql::functions::IsValidTime(gsql_timestamp)) {
    return kInvalidTimestamp;
  }
  return gsql_timestamp;
}

absl::StatusOr<DateADT> PgToDate(absl::string_view date_string,
                                 absl::string_view date_format) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum date_string_in_datum,
      CheckedPgStringToDatum(std::string(date_string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum date_format_in_datum,
      CheckedPgStringToDatum(std::string(date_format).c_str(), TEXTOID));

  absl::StatusOr<Datum> pg_date_or_error =
      postgres_translator::CheckedOidFunctionCall2(
          F_TO_DATE, date_string_in_datum, date_format_in_datum);
  // Remaps the pg out of range error message
  if (is_out_of_range_error(pg_date_or_error.status())) {
    return kInvalidDate;
  }
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_date, pg_date_or_error);
  ZETASQL_ASSIGN_OR_RETURN(int32_t gsql_date,
                   SafePgDateOffsetToGsqlDateOffset(pg_date));
  if (!IsValidDate(gsql_date)) {
    return kInvalidDate;
  }

  return gsql_date;
}

absl::StatusOr<DateADT> DateMii(DateADT date, int64_t days) {
  ZETASQL_ASSIGN_OR_RETURN(int32_t pg_date_offset,
                   SafeGsqlDateOffsetToPgDateOffset(date));
  int32_t pg_date;
  absl::Status subtraction_status =
      SafeSubtract(pg_date_offset, days, &pg_date);
  if (!subtraction_status.ok()) {
    return kInvalidDate;
  }
  ZETASQL_ASSIGN_OR_RETURN(int32_t gsql_date,
                   SafePgDateOffsetToGsqlDateOffset(pg_date));
  if (!IsValidDate(gsql_date)) {
    return kInvalidDate;
  }
  return gsql_date;
}

absl::StatusOr<int32_t> DatePli(int32_t date, int64_t days) {
  ZETASQL_ASSIGN_OR_RETURN(int32_t pg_date_offset,
                   SafeGsqlDateOffsetToPgDateOffset(date));
  int32_t pg_date;
  absl::Status add_status = SafeAdd(pg_date_offset, days, &pg_date);
  if (!add_status.ok()) {
    return kInvalidDate;
  }
  ZETASQL_ASSIGN_OR_RETURN(int32_t gsql_date,
                   SafePgDateOffsetToGsqlDateOffset(pg_date));
  if (!IsValidDate(gsql_date)) {
    return kInvalidDate;
  }
  return gsql_date;
}

absl::StatusOr<absl::Time> ToTimestamp(absl::string_view timestamp_string,
                                       absl::string_view timestamp_format) {
  ZETASQL_ASSIGN_OR_RETURN(
      Datum timestamp_string_datum,
      CheckedPgStringToDatum(std::string(timestamp_string).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(
      Datum timestamp_format_datum,
      CheckedPgStringToDatum(std::string(timestamp_format).c_str(), TEXTOID));

  absl::StatusOr<Datum> timestamp_or_error =
      postgres_translator::CheckedOidFunctionCall2(F_TO_TIMESTAMP_TEXT_TEXT,
                                                   timestamp_string_datum,
                                                   timestamp_format_datum);
  return TimestampFromDatumOr(timestamp_or_error);
}

absl::StatusOr<std::unique_ptr<std::string>> PgTimestampTzToChar(
    absl::Time timestamp, absl::string_view format) {
  Datum timestamp_in_datum = AbslTimeToPgTimestamptz(timestamp);
  ZETASQL_ASSIGN_OR_RETURN(
      Datum format_in_datum,
      CheckedPgStringToDatum(std::string(format).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(
      Datum formatted_timestamp_datum,
      postgres_translator::CheckedNullableOidFunctionCall2(
          F_TO_CHAR_TIMESTAMPTZ_TEXT, timestamp_in_datum, format_in_datum));

  if (formatted_timestamp_datum == NULL_DATUM) {
    return nullptr;
  }
  ZETASQL_ASSIGN_OR_RETURN(char* formatted_timestamp,
                   CheckedPgTextDatumGetCString(formatted_timestamp_datum));

  return std::make_unique<std::string>(formatted_timestamp);
}

absl::StatusOr<int32_t> PgDateIn(absl::string_view date_string) {
  ZETASQL_ASSIGN_OR_RETURN(Type date_type, CheckedPgTypeidType(DATEOID));
  // StringTypeDatum expects the string to be NUL terminated, so the string_view
  // must be copied into a NUL terminated std::string.
  absl::StatusOr<Datum> pg_date_or_error =
      postgres_translator::CheckedPgStringTypeDatum(
          date_type, const_cast<char*>(std::string(date_string).c_str()),
          /*atttypmod=*/-1);

  return DateFromDatumOr(pg_date_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzIn(absl::string_view timestamp_string) {
  ZETASQL_ASSIGN_OR_RETURN(Type timestamptz_type, CheckedPgTypeidType(TIMESTAMPTZOID));
  // StringTypeDatum expects the string to be NUL terminated, so the string_view
  // must be copied into a NUL terminated std::string.
  absl::StatusOr<Datum> pg_timestamptz_or_error =
      postgres_translator::CheckedPgStringTypeDatum(
          timestamptz_type,
          const_cast<char*>(std::string(timestamp_string).c_str()),
          /*atttypmod=*/-1);

  return TimestampFromDatumOr(pg_timestamptz_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzAdd(absl::Time input_time,
                                            absl::string_view interval_string) {
  Datum input_timestamptz = AbslTimeToPgTimestamptz(input_time);

  ZETASQL_ASSIGN_OR_RETURN(Type interval_type, CheckedPgTypeidType(INTERVALOID));
  absl::StatusOr<Datum> pg_interval_or_error =
      postgres_translator::CheckedPgStringTypeDatum(
          interval_type,
          const_cast<char*>(std::string(interval_string).c_str()),
          /*atttypmod=*/-1);
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_interval, pg_interval_or_error);

  absl::StatusOr<Datum> pg_timestamptz_or_error = CheckedOidFunctionCall2(
      F_TIMESTAMPTZ_PL_INTERVAL, input_timestamptz, pg_interval);

  return TimestampFromDatumOr(pg_timestamptz_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzSubtract(
    absl::Time input_time, absl::string_view interval_string) {
  Datum input_timestamptz = AbslTimeToPgTimestamptz(input_time);

  ZETASQL_ASSIGN_OR_RETURN(Type interval_type, CheckedPgTypeidType(INTERVALOID));
  absl::StatusOr<Datum> pg_interval_or_error =
      postgres_translator::CheckedPgStringTypeDatum(
          interval_type,
          const_cast<char*>(std::string(interval_string).c_str()),
          /*atttypmod=*/-1);
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_interval, pg_interval_or_error);

  absl::StatusOr<Datum> pg_timestamptz_or_error = CheckedOidFunctionCall2(
      F_TIMESTAMPTZ_MI_INTERVAL, input_timestamptz, pg_interval);

  return TimestampFromDatumOr(pg_timestamptz_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzBin(absl::string_view stride,
                                            absl::Time source,
                                            absl::Time origin) {
  ZETASQL_ASSIGN_OR_RETURN(Type interval_type, CheckedPgTypeidType(INTERVALOID));
  absl::StatusOr<Datum> pg_interval_or_error =
      postgres_translator::CheckedPgStringTypeDatum(
          interval_type, const_cast<char*>(std::string(stride).c_str()),
          /*atttypmod=*/-1);
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_interval, pg_interval_or_error);

  Datum pg_source = AbslTimeToPgTimestamptz(source);
  Datum pg_origin = AbslTimeToPgTimestamptz(origin);

  absl::StatusOr<Datum> pg_timestamptz_or_error =
      CheckedOidFunctionCall3(F_DATE_BIN_INTERVAL_TIMESTAMPTZ_TIMESTAMPTZ,
                              pg_interval, pg_source, pg_origin);

  return TimestampFromDatumOr(pg_timestamptz_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzTrunc(absl::string_view field,
                                              absl::Time source) {
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_field,
                   CheckedPgStringToDatum(std::string(field).c_str(), TEXTOID));
  Datum pg_source = AbslTimeToPgTimestamptz(source);
  absl::StatusOr<Datum> pg_timestamptz_or_error = CheckedOidFunctionCall2(
      F_DATE_TRUNC_TEXT_TIMESTAMPTZ, pg_field, pg_source);

  return TimestampFromDatumOr(pg_timestamptz_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzTrunc(absl::string_view field,
                                              absl::Time source,
                                              absl::string_view timezone) {
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_field,
                   CheckedPgStringToDatum(std::string(field).c_str(), TEXTOID));
  Datum pg_source = AbslTimeToPgTimestamptz(source);
  ZETASQL_ASSIGN_OR_RETURN(
      Datum pg_timezone,
      CheckedPgStringToDatum(std::string(timezone).c_str(), TEXTOID));
  absl::StatusOr<Datum> pg_timestamptz_or_error = CheckedOidFunctionCall3(
      F_DATE_TRUNC_TEXT_TIMESTAMPTZ_TEXT, pg_field, pg_source, pg_timezone);

  return TimestampFromDatumOr(pg_timestamptz_or_error);
}

absl::StatusOr<absl::Cord> PgTimestamptzExtract(absl::string_view field,
                                                absl::Time source) {
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_field,
                   CheckedPgStringToDatum(std::string(field).c_str(), TEXTOID));
  Datum pg_source = AbslTimeToPgTimestamptz(source);
  ZETASQL_ASSIGN_OR_RETURN(
      Datum result,
      CheckedOidFunctionCall2(F_EXTRACT_TEXT_TIMESTAMPTZ, pg_field, pg_source));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_string_result,
                   CheckedOidFunctionCall1(F_NUMERIC_OUT, result));
  absl::string_view string_value = DatumGetCString(numeric_string_result);
  ZETASQL_ASSIGN_OR_RETURN(std::string normalized_numeric,
                   NormalizePgNumeric(string_value));
  return absl::Cord(normalized_numeric);
}

absl::StatusOr<absl::Cord> PgDateExtract(absl::string_view field,
                                         int32_t source) {
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_field,
                   CheckedPgStringToDatum(std::string(field).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(int32_t pg_date_offset,
                   SafeGsqlDateOffsetToPgDateOffset(source));
  Datum pg_source = Int32GetDatum(pg_date_offset);
  ZETASQL_ASSIGN_OR_RETURN(Datum result, CheckedOidFunctionCall2(F_EXTRACT_TEXT_DATE,
                                                         pg_field, pg_source));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_string_result,
                   CheckedOidFunctionCall1(F_NUMERIC_OUT, result));
  absl::string_view string_value = DatumGetCString(numeric_string_result);
  ZETASQL_ASSIGN_OR_RETURN(std::string normalized_numeric,
                   NormalizePgNumeric(string_value));
  return absl::Cord(normalized_numeric);
}

absl::StatusOr<zetasql::IntervalValue> IntervalFromDatumOr(
    absl::StatusOr<Datum>& interval_or_error) {
  // Remaps the pg out of range error message
  if (interval_or_error.status().code() == absl::StatusCode::kInvalidArgument &&
      absl::StrContains(interval_or_error.status().message(),
                        kIntervalOutOfRangeMessage)) {
    return kInvalidInterval;
  }

  ZETASQL_ASSIGN_OR_RETURN(Datum pg_interval, interval_or_error);
  Interval* interval = DatumGetIntervalP(pg_interval);

  absl::StatusOr<zetasql::IntervalValue> gsql_interval =
      zetasql::IntervalValue::FromMonthsDaysMicros(
          interval->month, interval->day, interval->time);
  if (gsql_interval.status().code() == absl::StatusCode::kOutOfRange) {
    // Remaps the gsql out of range error message to Postgres Interval error
    // message.
    return kInvalidInterval;
  }

  return gsql_interval;
}

absl::StatusOr<zetasql::IntervalValue> PgRoundIntervalPrecision(
    const zetasql::IntervalValue& interval) {
  // Postgres Interval type has microsecond precision, so if the interval has
  // no nano fractions, we can return it as is.
  if (ABSL_PREDICT_TRUE(interval.get_nano_fractions() == 0)) {
    return interval;
  }

  // `get_nano_fractions()` return values in the range [0, 999].
  int64_t nano_fractions = interval.get_nano_fractions();

  int64_t micros = interval.get_micros();
  bool is_negative = micros < 0;

  // Keep the sign of microseconds and nano fractions consistent.
  if (micros < 0) {
    micros += 1;
    nano_fractions -= zetasql::IntervalValue::kNanosInMicro;
  }

  micros = std::abs(micros);
  nano_fractions = std::abs(nano_fractions);

  micros =
      micros +
      (nano_fractions >= (zetasql::IntervalValue::kNanosInMicro / 2) ? 1 : 0);

  return zetasql::IntervalValue::FromMonthsDaysMicros(
      interval.get_months(), interval.get_days(),
      is_negative ? -micros : micros);
}

absl::StatusOr<Interval> ToPgInterval(
    const zetasql::IntervalValue& interval) {
  ZETASQL_ASSIGN_OR_RETURN(zetasql::IntervalValue rounded_interval,
                   PgRoundIntervalPrecision(interval));
  return Interval{.time = rounded_interval.get_micros(),
                  .day = static_cast<int32_t>(interval.get_days()),
                  .month = static_cast<int32_t>(interval.get_months())};
}

absl::StatusOr<absl::Time> PgTimestamptzAdd(
    absl::Time input_time, const zetasql::IntervalValue& interval) {
  Datum input_timestamptz = AbslTimeToPgTimestamptz(input_time);
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_datum = IntervalPGetDatum(&pg_interval);

  absl::StatusOr<Datum> datum_or_error = CheckedOidFunctionCall2(
      F_TIMESTAMPTZ_PL_INTERVAL, input_timestamptz, interval_datum);

  return TimestampFromDatumOr(datum_or_error);
}

absl::StatusOr<absl::Time> PgTimestamptzSubtract(
    absl::Time input_time, const zetasql::IntervalValue& interval) {
  Datum input_timestamptz = AbslTimeToPgTimestamptz(input_time);
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_datum = IntervalPGetDatum(&pg_interval);

  absl::StatusOr<Datum> datum_or_error = CheckedOidFunctionCall2(
      F_TIMESTAMPTZ_MI_INTERVAL, input_timestamptz, interval_datum);

  return TimestampFromDatumOr(datum_or_error);
}

absl::StatusOr<zetasql::IntervalValue> PgMakeInterval(
    int64_t years, int64_t months, int64_t weeks, int64_t days, int64_t hours,
    int64_t minutes, double seconds) {
  auto convert_to_int32 = [](int64_t value) -> absl::StatusOr<int32_t> {
    if (value < std::numeric_limits<int32_t>::min() ||
        value > std::numeric_limits<int32_t>::max()) {
      return kInvalidInterval;
    }
    return static_cast<int32_t>(value);
  };

  ZETASQL_ASSIGN_OR_RETURN(int32_t years_int32, convert_to_int32(years));
  ZETASQL_ASSIGN_OR_RETURN(int32_t months_int32, convert_to_int32(months));
  ZETASQL_ASSIGN_OR_RETURN(int32_t weeks_int32, convert_to_int32(weeks));
  ZETASQL_ASSIGN_OR_RETURN(int32_t days_int32, convert_to_int32(days));
  ZETASQL_ASSIGN_OR_RETURN(int32_t hours_int32, convert_to_int32(hours));
  ZETASQL_ASSIGN_OR_RETURN(int32_t minutes_int32, convert_to_int32(minutes));

  absl::StatusOr<Datum> datum_or_error =
      postgres_translator::CheckedOidFunctionCall7(
          F_MAKE_INTERVAL, Int32GetDatum(years_int32),
          Int32GetDatum(months_int32), Int32GetDatum(weeks_int32),
          Int32GetDatum(days_int32), Int32GetDatum(hours_int32),
          Int32GetDatum(minutes_int32), Float8GetDatum(seconds));

  return IntervalFromDatumOr(datum_or_error);
}

absl::StatusOr<zetasql::IntervalValue> PgIntervalMultiply(
    const zetasql::IntervalValue& interval, double multiplier) {
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_datum = IntervalPGetDatum(&pg_interval);
  Datum multiplier_datum = Float8GetDatum(multiplier);

  absl::StatusOr<Datum> datum_or_error =
      postgres_translator::CheckedOidFunctionCall2(
          F_INTERVAL_MUL, interval_datum, multiplier_datum);

  return IntervalFromDatumOr(datum_or_error);
}

absl::StatusOr<zetasql::IntervalValue> PgIntervalDivide(
    const zetasql::IntervalValue& interval, double divisor) {
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_datum = IntervalPGetDatum(&pg_interval);
  Datum divisor_datum = Float8GetDatum(divisor);

  absl::StatusOr<Datum> datum_or_error =
      postgres_translator::CheckedOidFunctionCall2(
          F_INTERVAL_DIV, interval_datum, divisor_datum);

  return IntervalFromDatumOr(datum_or_error);
}

absl::StatusOr<absl::Cord> PgIntervalExtract(
    absl::string_view field, const zetasql::IntervalValue& interval) {
  ZETASQL_ASSIGN_OR_RETURN(Datum pg_field,
                   CheckedPgStringToDatum(std::string(field).c_str(), TEXTOID));
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_datum = IntervalPGetDatum(&pg_interval);

  ZETASQL_ASSIGN_OR_RETURN(Datum result,
                   CheckedOidFunctionCall2(F_EXTRACT_TEXT_INTERVAL, pg_field,
                                           interval_datum));
  ZETASQL_ASSIGN_OR_RETURN(Datum numeric_string_result,
                   CheckedOidFunctionCall1(F_NUMERIC_OUT, result));
  absl::string_view string_value = DatumGetCString(numeric_string_result);

  ZETASQL_ASSIGN_OR_RETURN(std::string normalized_numeric,
                   NormalizePgNumeric(string_value));
  return absl::Cord(normalized_numeric);
}

absl::StatusOr<zetasql::IntervalValue> PgIntervalIn(
    absl::string_view interval_string) {
  ZETASQL_ASSIGN_OR_RETURN(Type interval_type, CheckedPgTypeidType(INTERVALOID));
  // StringTypeDatum expects the string to be NULL terminated, so the
  // string_view must be copied into a NULL terminated std::string.
  absl::StatusOr<Datum> datum_or_error =
      postgres_translator::CheckedPgStringTypeDatum(
          interval_type,
          const_cast<char*>(std::string(interval_string).c_str()),
          /*atttypmod=*/-1);

  return IntervalFromDatumOr(datum_or_error);
}

absl::StatusOr<std::string> PgIntervalOut(
    const zetasql::IntervalValue& interval) {
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_in_datum = IntervalPGetDatum(&pg_interval);

  ZETASQL_ASSIGN_OR_RETURN(Datum interval_out_datum,
                   postgres_translator::CheckedOidFunctionCall1(
                       F_INTERVAL_OUT, interval_in_datum));

  return DatumGetCString(interval_out_datum);
}

absl::StatusOr<std::unique_ptr<std::string>> PgIntervalToChar(
    const zetasql::IntervalValue& interval, absl::string_view format) {
  ZETASQL_ASSIGN_OR_RETURN(Interval pg_interval, ToPgInterval(interval));
  Datum interval_in_datum = IntervalPGetDatum(&pg_interval);
  ZETASQL_ASSIGN_OR_RETURN(
      Datum format_in_datum,
      CheckedPgStringToDatum(std::string(format).c_str(), TEXTOID));

  ZETASQL_ASSIGN_OR_RETURN(
      Datum formatted_interval_datum,
      postgres_translator::CheckedNullableOidFunctionCall2(
          F_TO_CHAR_INTERVAL_TEXT, interval_in_datum, format_in_datum));

  if (formatted_interval_datum == NULL_DATUM) {
    return nullptr;
  }
  ZETASQL_ASSIGN_OR_RETURN(char* formatted_interval,
                   CheckedPgTextDatumGetCString(formatted_interval_datum));

  return std::make_unique<std::string>(formatted_interval);
}

}  // namespace postgres_translator::function_evaluators
