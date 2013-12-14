#include <queue>

#include "htCustomScanner.h"

htCustomScanner::htCustomScanner(ThriftClientPtr client,
				std::string ns,
				std::string table,
				std::set<std::string> colls,
				size_t lines_in_buffer)
{
	m_lines_in_buffer = lines_in_buffer;
	m_client = client;
	m_colls = colls;
	
	Hypertable::ThriftGen::Namespace m_ns = client->namespace_open(ns);
	m_s = client->open_scanner(m_ns, table, m_ss);
	
	parseMore();
}

void htCustomScanner::loadMore()
{
	std::vector<Hypertable::ThriftGen::Cell> cells;
	m_client->scanner_get_cells(cells, m_s);
	
	if (cells.size()==0)
	{
		return;
	}
	
	for (int i = 0; i<cells.size(); i++)
	{
			cell_buffer.push(htCell(cells[i].key.row,
									cells[i].key.column_family,
									cells[i].value));
	}
	cells.clear();
	
	if (cell_buffer.size()<255)
		loadMore();
}

void htCustomScanner::parseMore()
{	
	//std::cout << "ParseMore\n";
	loadMore();
	if (cell_buffer.size() == 0)
	{
		return;
	}
	
	htLine cur_line;
	std::string cur_key;
	
	int nline = 0;
	
	for (int i = 0; cell_buffer.size()!=0; i++)
	{	 
		htCell cell = cell_buffer.front();
		// if need this coll
		if (	m_colls.find(cell.coll) != m_colls.end())
		{			
			// if key changed or end
			if ( (i!=0 && cell.key != cur_key) || cell_buffer.size()==0)
			{
				// end
				if (cell_buffer.size()==0)
				{
					cur_line.cells[cell.coll] = cell.value;
				}
				// push to line buffer
				cur_line.key = cur_key;
				line_buffer.push(cur_line);
				cur_key = cell.key;
				nline++;
				// end or got enough lines
				if (nline >= m_lines_in_buffer || cell_buffer.size()==0)
				{
					break;
				}
			}
			if (i==0) // first key
			{
				cur_key = cell.key;
			}
			cur_line.cells[cell.coll] = cell.value;
			
		}
		
		cell_buffer.pop();
		
		if (cell_buffer.size() < 255) // keep enough cells in buffer
			loadMore();
	}
	if (cell_buffer.size()==0)
	{
		cur_line.key = cur_key;
		line_buffer.push(cur_line);
	}
}

htLine htCustomScanner::getNextLine()
{
	if (line_buffer.size()!=0)
	{
		htLine line = line_buffer.front();
		line_buffer.pop();
		if (line_buffer.size() < m_lines_in_buffer)
		{
			parseMore();
		}
		return line;
	}
	else
	{
		throw "htCollScanner::getNextCell() line_buffer is empty";
	}
}

bool htCustomScanner::end()
{
	return line_buffer.size()==0 && cell_buffer.size()==0;
}

htCustomScanner::~htCustomScanner()
{
	m_client->scanner_close(m_s);
}
