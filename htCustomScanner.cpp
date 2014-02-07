#include <queue>

#include "htCustomScanner.h"

htCustomScanner::htCustomScanner(htConnPoolPtr _conn_pool,
				const std::string &_ns,
				const std::string &_table,
				const std::set<std::string> &_colls,
				size_t _lines_in_buffer)
{
	m_conn_pool = _conn_pool;
	m_lines_in_buffer = _lines_in_buffer;
	m_colls = _colls;
	m_table = _table;
	m_sns = _ns;
		
	reset();
	parseMore();
}

void htCustomScanner::reset()
{
	reset(KeyRange::getEmptyRange());
}

void htCustomScanner::reset(const KeyRange &_range)
{
	if (_range.ok()) {
		std::string err = "htCustomScanner::reset range setting not implemented\n";
		std::cout << err;
		throw err;
	}
	
	htConnPool::htSession sess = m_conn_pool->get();
	m_ns = sess.client->namespace_open(m_sns);
	m_s = sess.client->open_scanner(m_ns, m_table, m_ss);
}

void htCustomScanner::loadMore(htConnPool::htSession &_sess)
{
	std::vector<Hypertable::ThriftGen::Cell> cells;
	_sess.client->scanner_get_cells(cells, m_s);
	
	if (cells.size()==0)
		return;
	
	for (int i = 0; i<cells.size(); i++) {
			cell_buffer.push(htCell(cells[i].key.row,
									cells[i].key.column_family,
									cells[i].value));
	}
	cells.clear();
	
	if (cell_buffer.size()<255)
		loadMore(_sess);
}

void htCustomScanner::parseMore()
{	
	//std::cout << "ParseMore\n";
	{
		htConnPool::htSession sess = m_conn_pool->get();
		loadMore(sess);
	}
	
	if (cell_buffer.size() == 0)
	{
		return;
	}
	
	htLine cur_line;
	std::string cur_key;
	
	int nline = 0;
	
	for (int i = 0; cell_buffer.size()!=0; i++) {	 
		htCell cell = cell_buffer.front();
		// if need this coll
		if (	m_colls.find(cell.coll) != m_colls.end()) {			
			// if key changed or end
			if ( (i!=0 && cell.key != cur_key) || cell_buffer.size()==0) {
				// end
				if (cell_buffer.size()==0) {
					cur_line.cells[cell.coll] = cell.value;
				}
				// push to line buffer
				cur_line.key = cur_key;
				if (cur_key.size()>=1)
					line_buffer.push(cur_line);
				cur_key = cell.key;
				nline++;
				// end or got enough lines
				if (nline >= m_lines_in_buffer || cell_buffer.size()==0) {
					break;
				}
			}
			if (i==0) { // first key
				cur_key = cell.key;
			}
			cur_line.cells[cell.coll] = cell.value;
		}
		
		cell_buffer.pop();
		
		if (cell_buffer.size() < 255) { // keep enough cells in buffer 
			htConnPool::htSession sess = m_conn_pool->get();
			loadMore(sess);
		}
	}
	
	if (cell_buffer.size()==0) {
		cur_line.key = cur_key;
		if (cur_key.size()>=1)
			line_buffer.push(cur_line);
	}
}

htLine &htCustomScanner::getNextLine(htLine &line)
{
	if (line_buffer.size()!=0) {
		line = line_buffer.front();
		line_buffer.pop();
		if (line_buffer.size() < m_lines_in_buffer) {
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
	htConnPool::htSession sess = m_conn_pool->get();
	sess.client->scanner_close(m_s);
}
