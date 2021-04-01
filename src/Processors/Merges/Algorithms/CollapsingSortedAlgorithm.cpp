#include <Processors/Merges/Algorithms/CollapsingSortedAlgorithm.h>

#include <Columns/ColumnsNumber.h>
#include <Common/FieldVisitors.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <common/logger_useful.h>

/// Maximum number of messages about incorrect data in the log.
#define MAX_ERROR_MESSAGES 10

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

CollapsingSortedAlgorithm::CollapsingSortedAlgorithm(
    const Block & header,
    size_t num_inputs,
    SortDescription description_,
    const String & sign_column,
    bool only_positive_sign_,
    size_t max_block_size,
    WriteBuffer * out_row_sources_buf_,
    bool use_average_block_sizes,
    Poco::Logger * log_)
    : IMergingAlgorithmWithSharedChunks(num_inputs, std::move(description_), out_row_sources_buf_, max_row_refs)
    , merged_data(header.cloneEmptyColumns(), use_average_block_sizes, max_block_size)
    , sign_column_number(header.getPositionByName(sign_column))
    , only_positive_sign(only_positive_sign_)
    , log(log_)
{
}

void CollapsingSortedAlgorithm::reportIncorrectData()
{
    if (!log)
        return;

    std::stringstream s;
    auto & sort_columns = *last_row.sort_columns;
    for (size_t i = 0, size = sort_columns.size(); i < size; ++i)
    {
        if (i != 0)
            s << ", ";
        s << applyVisitor(FieldVisitorToString(), (*sort_columns[i])[last_row.row_num]);
    }
    s << ").";

    /** Fow now we limit ourselves to just logging such situations,
      *  since the data is generated by external programs.
      * With inconsistent data, this is an unavoidable error that can not be easily corrected by admins. Therefore Warning.
      */
    LOG_WARNING(log,
        "Incorrect data: number of rows with sign = 1 ({}) differs with number of rows with sign = -1 ({}) by more than one (for key: {}).",
        count_positive, count_negative, s.str());
}

void CollapsingSortedAlgorithm::insertRow(RowRef & row)
{
    merged_data.insertRow(*row.all_columns, row.row_num, row.owned_chunk->getNumRows());
}

std::optional<Chunk> CollapsingSortedAlgorithm::insertRows()
{
    if (count_positive == 0 && count_negative == 0)
    {
        /// No input rows have been read.
        return {};
    }

    std::optional<Chunk> res;

    if (last_is_positive || count_positive != count_negative)
    {
        if (count_positive <= count_negative && !only_positive_sign)
        {
            insertRow(first_negative_row);

            if (out_row_sources_buf)
                current_row_sources[first_negative_pos].setSkipFlag(false);
        }

        if (count_positive >= count_negative)
        {
            if (merged_data.hasEnoughRows())
                res = merged_data.pull();

            insertRow(last_positive_row);

            if (out_row_sources_buf)
                current_row_sources[last_positive_pos].setSkipFlag(false);
        }

        if (!(count_positive == count_negative || count_positive + 1 == count_negative || count_positive == count_negative + 1))
        {
            if (count_incorrect_data < MAX_ERROR_MESSAGES)
                reportIncorrectData();
            ++count_incorrect_data;
        }
    }

    first_negative_row.clear();
    last_positive_row.clear();

    if (out_row_sources_buf)
        out_row_sources_buf->write(
                reinterpret_cast<const char *>(current_row_sources.data()),
                current_row_sources.size() * sizeof(RowSourcePart));

    return res;
}

IMergingAlgorithm::Status CollapsingSortedAlgorithm::merge()
{
    /// Rare case, which may happen when index_granularity is 1, but we needed to insert 2 rows inside insertRows().
    if (merged_data.hasEnoughRows())
        return Status(merged_data.pull());

    /// Take rows in required order and put them into `merged_data`, while the rows are no more than `max_block_size`
    while (queue.isValid())
    {
        auto current = queue.current();

        if (current->isLast() && skipLastRowFor(current->order))
        {
            /// Get the next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }

        Int8 sign = assert_cast<const ColumnInt8 &>(*current->all_columns[sign_column_number]).getData()[current->pos];

        RowRef current_row;
        setRowRef(current_row, current);

        if (last_row.empty())
            setRowRef(last_row, current);

        bool key_differs = !last_row.hasEqualSortColumnsWith(current_row);
        if (key_differs)
        {
            /// if there are enough rows and the last one is calculated completely
            if (merged_data.hasEnoughRows())
                return Status(merged_data.pull());

            /// We write data for the previous primary key.
            auto res = insertRows();

            current_row.swap(last_row);

            count_negative = 0;
            count_positive = 0;

            current_pos = 0;
            first_negative_pos = 0;
            last_positive_pos = 0;
            current_row_sources.resize(0);

            /// Here we can return ready chunk.
            /// Next iteration, last_row == current_row, and all the counters are zeroed.
            /// So, current_row should be correctly processed.
            if (res)
                return Status(std::move(*res));
        }

        /// Initially, skip all rows. On insert, unskip "corner" rows.
        if (out_row_sources_buf)
            current_row_sources.emplace_back(current.impl->order, true);

        if (sign == 1)
        {
            ++count_positive;
            last_is_positive = true;

            setRowRef(last_positive_row, current);
            last_positive_pos = current_pos;
        }
        else if (sign == -1)
        {
            if (!count_negative)
            {
                setRowRef(first_negative_row, current);
                first_negative_pos = current_pos;
            }

            ++count_negative;
            last_is_positive = false;
        }
        else
            throw Exception("Incorrect data: Sign = " + toString(sign) + " (must be 1 or -1).",
                            ErrorCodes::INCORRECT_DATA);

        ++current_pos;

        if (!current->isLast())
        {
            queue.next();
        }
        else
        {
            /// We take next block from the corresponding source, if there is one.
            queue.removeTop();
            return Status(current.impl->order);
        }
    }

    if (auto res = insertRows())
    {
        /// Queue is empty, and we have inserted all the rows.
        /// Set counter to zero so that insertRows() will return immediately next time.
        count_positive = 0;
        count_negative = 0;
        return Status(std::move(*res));
    }

    return Status(merged_data.pull(), true);
}

}
