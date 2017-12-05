#include <memory>

#include "storage/table.h"

template<class TableType>
struct TableDeleter;

template<class TableType>
class UniqueTable {
public:
    UniqueTable(TableType* tbl)
        : m_table(tbl)
    {
        tbl->incrementRefcount();
    }

    TableType* get() {
        return m_table.get();
    }

    const TableType* get() const {
        return m_table.get();
    }

    TableType& operator*() {
        return m_table.operator*();
    }

    const TableType& operator*() const {
        return m_table.operator*();
    }

    TableType* operator->() {
        return m_table.operator->();
    }

    const TableType* operator->() const {
        return m_table.operator->();
    }

    void reset(TableType* newTable = NULL) {
        m_table.reset(newTable);
    }

private:
    std::unique_ptr<TableType, TableDeleter<TableType>> m_table;
};

template<class TableType>
UniqueTable<TableType> makeUniqueTable(TableType* tbl) {
    return UniqueTable<TableType>(tbl);
}

template<class TableType>
struct TableDeleter {
    void operator()(TableType* tbl) const {
        tbl->decrementRefcount();
    }
};
