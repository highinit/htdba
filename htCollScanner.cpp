#include <queue>

#include "htCollScanner.h"

htCollScanner::htCollScanner(ThriftClientPtr client,
				std::string ns,
				std::string table,
				std::string coll):
	m_client(client),
	m_coll(coll),
	m_table(table)
{
	m_ns = client->namespace_open(ns);
	reset();
}

void htCollScanner::loadMore()
{
	std::vector<Hypertable::ThriftGen::Cell> cells;
	m_client->next_cells(cells, m_s);
	
	if (cells.size()==0)
	{
		return;
	}
	
	for (int i = 0; i<cells.size(); i++)
		if (	cells[i].key.column_family == m_coll)
	{
			buffer.push(KeyValue(cells[i].key.row, cells[i].value));
	}
	cells.clear();
}

KeyValue htCollScanner::getNextCell()
{
	if (buffer.size()!=0)
	{
		KeyValue kv = buffer.front();
		buffer.pop();
		if (buffer.size()<10)
		{
			loadMore();
		}
		return kv;
	}
	else
	{
		throw "htCollScanner::getNextCell() buffer empty";
	}
}

void htCollScanner::reset()
{
	m_s = m_client->open_scanner(m_ns, m_table, m_ss);
	while (buffer.size()!=0)
		buffer.pop();
	loadMore();
}

void htCollScanner::reset(const KeyRange &range)
{
	if (range.ok())
	{
		Hypertable::ThriftGen::RowInterval interval;
		interval.__isset.start_row = true;
		interval.__isset.end_row = true;
		interval.__isset.start_inclusive = true;
		interval.__isset.end_inclusive = true;
		interval.start_row = range.beg;
		interval.end_row = range.end;
		interval.start_inclusive = true;
		interval.end_inclusive = true;



		m_ss.__isset.row_intervals = true;
		std::vector<Hypertable::ThriftGen::RowInterval> intervals;
		intervals.push_back(interval);
		m_ss.__set_row_intervals(intervals);
	}
	reset();
}

bool htCollScanner::end()
{
	return buffer.size()==0;
}

htCollScanner::~htCollScanner()
{
	m_client->scanner_close(m_s);
}


KeyValue htCollScannerConc::getNextCell()
{
	lock.lock();
	KeyValue kv = htCollScanner::getNextCell();
	lock.unlock();
	return kv;
}

bool htCollScannerConc::end()
{
	lock.lock();
	bool isend = htCollScanner::end();
	lock.unlock();
	return isend;
}
