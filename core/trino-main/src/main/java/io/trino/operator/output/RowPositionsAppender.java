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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.ValueBlock;
import io.trino.spi.type.RowType;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.output.PositionsAppenderUtil.calculateBlockResetSize;
import static io.trino.operator.output.PositionsAppenderUtil.calculateNewArraySize;
import static io.trino.spi.block.RowBlock.fromFieldBlocks;
import static java.util.Objects.requireNonNull;

public class RowPositionsAppender
        implements PositionsAppender
{
    private static final int INSTANCE_SIZE = instanceSize(RowPositionsAppender.class);
    private final UnnestingPositionsAppender[] fieldAppenders;
    private int initialEntryCount;
    private boolean initialized;

    private int positionCount;
    private boolean hasNullRow;
    private boolean hasNonNullRow;
    private boolean[] rowIsNull = new boolean[0];
    private long retainedSizeInBytes = -1;
    private long sizeInBytes = -1;

    public static RowPositionsAppender createRowAppender(
            PositionsAppenderFactory positionsAppenderFactory,
            RowType type,
            int expectedPositions,
            long maxPageSizeInBytes)
    {
        UnnestingPositionsAppender[] fields = new UnnestingPositionsAppender[type.getFields().size()];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = positionsAppenderFactory.create(type.getFields().get(i).getType(), expectedPositions, maxPageSizeInBytes);
        }
        return new RowPositionsAppender(fields, expectedPositions);
    }

    private RowPositionsAppender(UnnestingPositionsAppender[] fieldAppenders, int expectedPositions)
    {
        this.fieldAppenders = requireNonNull(fieldAppenders, "fields is null");
        this.initialEntryCount = expectedPositions;
        resetSize();
    }

    @Override
    public void append(IntArrayList positions, ValueBlock block)
    {
        checkArgument(block instanceof RowBlock, "Block must be instance of %s", RowBlock.class);

        if (positions.isEmpty()) {
            return;
        }
        ensureCapacity(positions.size());
        RowBlock sourceRowBlock = (RowBlock) block;
        IntArrayList nonNullPositions;
        if (sourceRowBlock.mayHaveNull()) {
            nonNullPositions = processNullablePositions(positions, sourceRowBlock);
            hasNullRow |= nonNullPositions.size() < positions.size();
            hasNonNullRow |= !nonNullPositions.isEmpty();
        }
        else {
            // the source Block does not have nulls
            nonNullPositions = processNonNullablePositions(positions, sourceRowBlock);
            hasNonNullRow = true;
        }

        List<Block> fieldBlocks = sourceRowBlock.getChildren();
        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldAppenders[i].append(nonNullPositions, fieldBlocks.get(i));
        }

        positionCount += positions.size();
        resetSize();
    }

    @Override
    public void appendRle(ValueBlock value, int rlePositionCount)
    {
        checkArgument(value instanceof RowBlock, "Block must be instance of %s", RowBlock.class);

        ensureCapacity(rlePositionCount);
        RowBlock sourceRowBlock = (RowBlock) value;
        if (sourceRowBlock.isNull(0)) {
            // append rlePositionCount nulls
            Arrays.fill(rowIsNull, positionCount, positionCount + rlePositionCount, true);
            hasNullRow = true;
        }
        else {
            // append not null row value
            List<Block> fieldBlocks = sourceRowBlock.getChildren();
            int fieldPosition = sourceRowBlock.getFieldBlockOffset(0);
            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldAppenders[i].appendRle(fieldBlocks.get(i).getSingleValueBlock(fieldPosition), rlePositionCount);
            }
            hasNonNullRow = true;
        }
        positionCount += rlePositionCount;
        resetSize();
    }

    @Override
    public void append(int position, ValueBlock value)
    {
        checkArgument(value instanceof RowBlock, "Block must be instance of %s", RowBlock.class);

        ensureCapacity(1);
        RowBlock sourceRowBlock = (RowBlock) value;
        if (sourceRowBlock.isNull(position)) {
            rowIsNull[positionCount] = true;
            hasNullRow = true;
        }
        else {
            // append not null row value
            List<Block> fieldBlocks = sourceRowBlock.getChildren();
            int fieldPosition = sourceRowBlock.getFieldBlockOffset(position);
            for (int i = 0; i < fieldAppenders.length; i++) {
                fieldAppenders[i].append(fieldPosition, fieldBlocks.get(i));
            }
            hasNonNullRow = true;
        }
        positionCount++;
        resetSize();
    }

    @Override
    public Block build()
    {
        Block[] fieldBlocks = new Block[fieldAppenders.length];
        for (int i = 0; i < fieldAppenders.length; i++) {
            fieldBlocks[i] = fieldAppenders[i].build();
        }
        Block result;
        if (hasNonNullRow) {
            result = fromFieldBlocks(positionCount, hasNullRow ? Optional.of(rowIsNull) : Optional.empty(), fieldBlocks);
        }
        else {
            Block nullRowBlock = fromFieldBlocks(1, Optional.of(new boolean[] {true}), fieldBlocks);
            result = RunLengthEncodedBlock.create(nullRowBlock, positionCount);
        }

        reset();
        return result;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        if (retainedSizeInBytes != -1) {
            return retainedSizeInBytes;
        }

        long size = INSTANCE_SIZE + sizeOf(rowIsNull);
        for (UnnestingPositionsAppender field : fieldAppenders) {
            size += field.getRetainedSizeInBytes();
        }

        retainedSizeInBytes = size;
        return size;
    }

    @Override
    public long getSizeInBytes()
    {
        if (sizeInBytes != -1) {
            return sizeInBytes;
        }

        long size = (Integer.BYTES + Byte.BYTES) * (long) positionCount;
        for (UnnestingPositionsAppender field : fieldAppenders) {
            size += field.getSizeInBytes();
        }

        sizeInBytes = size;
        return sizeInBytes;
    }

    private void reset()
    {
        initialEntryCount = calculateBlockResetSize(positionCount);
        initialized = false;
        rowIsNull = new boolean[0];
        positionCount = 0;
        hasNonNullRow = false;
        hasNullRow = false;
        resetSize();
    }

    private IntArrayList processNullablePositions(IntArrayList positions, RowBlock sourceRowBlock)
    {
        int[] nonNullPositions = new int[positions.size()];
        int nonNullPositionsCount = 0;

        for (int i = 0; i < positions.size(); i++) {
            int position = positions.getInt(i);
            boolean positionIsNull = sourceRowBlock.isNull(position);
            nonNullPositions[nonNullPositionsCount] = sourceRowBlock.getFieldBlockOffset(position);
            nonNullPositionsCount += positionIsNull ? 0 : 1;
            rowIsNull[positionCount + i] = positionIsNull;
        }

        return IntArrayList.wrap(nonNullPositions, nonNullPositionsCount);
    }

    private static IntArrayList processNonNullablePositions(IntArrayList positions, RowBlock sourceRowBlock)
    {
        int[] nonNullPositions = new int[positions.size()];
        for (int i = 0; i < positions.size(); i++) {
            nonNullPositions[i] = sourceRowBlock.getFieldBlockOffset(positions.getInt(i));
        }
        return IntArrayList.wrap(nonNullPositions);
    }

    private void ensureCapacity(int additionalCapacity)
    {
        if (rowIsNull.length <= positionCount + additionalCapacity) {
            int newSize;
            if (initialized) {
                newSize = calculateNewArraySize(rowIsNull.length);
            }
            else {
                newSize = initialEntryCount;
                initialized = true;
            }

            int newCapacity = Math.max(newSize, positionCount + additionalCapacity);
            rowIsNull = Arrays.copyOf(rowIsNull, newCapacity);
            resetSize();
        }
    }

    private void resetSize()
    {
        sizeInBytes = -1;
        retainedSizeInBytes = -1;
    }
}
