#pragma once

#include "clickhouse/columns/column.h"

namespace clickhouse {

/** */
template <typename T>
class ColumnVector : public Column {
public:
    ColumnVector()
        : Column(Type::CreateSimple<T>())
    {
    }

    /// Append one element to the column.
    void Append(const T& value) {
        data_.push_back(value);
    }

    void Append(ColumnRef column) override {
        if (auto col = column->As<ColumnVector<T>>()) {
            data_.insert(data_.end(), col->data_.begin(), col->data_.end());
        }
    }

    const T& operator [] (size_t n) const {
        return data_[n];
    }

    size_t Size() const override {
        return data_.size();
    }

    bool Load(CodedInputStream* input, size_t rows) override {
        data_.resize(rows);

        return input->ReadRaw(data_.data(), data_.size() * sizeof(T));
    }

    void Save(CodedOutputStream* output) override {
        output->WriteRaw(data_.data(), data_.size() * sizeof(T));
    }

    ColumnRef Slice(size_t begin, size_t len) override {
        if (begin >= data_.size()) {
            return ColumnRef();
        }

        len = std::min(len, data_.size() - begin);

        auto result = std::make_shared<ColumnVector<T>>();
        result->data_.assign(
            data_.begin() + begin, data_.begin() + (begin + len)
        );
        return result;
    }

protected:
    std::vector<T> data_;
};

using ColumnUInt8   = ColumnVector<uint8_t>;
using ColumnUInt16  = ColumnVector<uint16_t>;
using ColumnUInt32  = ColumnVector<uint32_t>;
using ColumnUInt64  = ColumnVector<uint64_t>;

using ColumnInt8    = ColumnVector<int8_t>;
using ColumnInt16   = ColumnVector<int16_t>;
using ColumnInt32   = ColumnVector<int32_t>;
using ColumnInt64   = ColumnVector<int64_t>;

using ColumnFloat32 = ColumnVector<float>;
using ColumnFloat64 = ColumnVector<double>;

}
