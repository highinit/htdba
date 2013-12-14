#include <queue>

#include "htCollScanner.h"

htCollScanner::htCollScanner(ThriftClientPtr client,
				std::string ns,
				std::string table,
				std::string coll):
	m_client(client),
	m_coll(coll)
{
	m_scanner_end = 0;
	Hypertable::ThriftGen::Namespace m_ns = client->namespace_open(ns);
	m_s = client->open_scanner(m_ns, table, m_ss);
	
	loadMore();
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
