/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.redis;
import com.google.common.primitives.SignedBytes;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeScaledValue;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.JSON;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToIntBits;

public class RediSearchPageSourceResultWriter
{
    public void appendTo(Type type, String value, BlockBuilder output)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            type.writeBoolean(output, Boolean.parseBoolean(value));
        }
        else if (javaType == long.class) {
            type.writeLong(output, getLong(type, value));
        }
        else if (javaType == double.class) {
            type.writeDouble(output, Double.parseDouble(value));
        }
        else if (javaType == Slice.class) {
            writeSlice(output, type, value);
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    "Unhandled type for " + javaType.getSimpleName() + ":" + type.getTypeSignature());
        }
    }

    private long getLong(Type type, String value)
    {
        if (type.equals(BIGINT)) {
            return Long.parseLong(value);
        }
        if (type.equals(INTEGER)) {
            return Integer.parseInt(value);
        }
        if (type.equals(SMALLINT)) {
            return Short.parseShort(value);
        }
        if (type.equals(TINYINT)) {
            return SignedBytes.checkedCast(Long.parseLong(value));
        }
        if (type.equals(REAL)) {
            return floatToIntBits((Float.parseFloat(value)));
        }
        if (type instanceof DecimalType) {
            return encodeShortScaledValue(new BigDecimal(value), ((DecimalType) type).getScale());
        }
        if (type.equals(DATE)) {
            return LocalDate.from(DateTimeFormatter.ISO_DATE.parse(value)).toEpochDay();
        }
        if (type.equals(TIMESTAMP_MILLIS)) {
            return Long.parseLong(value) * MICROSECONDS_PER_MILLISECOND;
        }
        if (type.equals(TIMESTAMP_TZ_MILLIS)) {
            return packDateTimeWithZone(Long.parseLong(value), UTC_KEY);
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR,
                "Unhandled type for " + type.getJavaType().getSimpleName() + ":" + type.getTypeSignature());
    }

    private void writeSlice(BlockBuilder output, Type type, String value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(value));
        }
        else if (type instanceof CharType) {
            type.writeSlice(output, truncateToLengthAndTrimSpaces(utf8Slice(value), (CharType) type));
        }
        else if (type instanceof DecimalType) {
            type.writeObject(output, encodeScaledValue(new BigDecimal(value), ((DecimalType) type).getScale()));
        }
        else if (type.getBaseName().equals(JSON)) {
            type.writeSlice(output, io.trino.plugin.base.util.JsonTypeUtil.jsonParse(utf8Slice(value)));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }
}
