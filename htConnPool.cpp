#include <vector>

#include "htConnPool.h"

htConnPool::htSession::htSession(ThriftClientPtr _client,
				hLockTicketPtr _lock_ticket)
{
	client = _client;
	lock_ticket = _lock_ticket;
}

htConnPool::htConnection::htConnection(ThriftClientPtr _client)
{
	client = _client;
}
	
htConnPool::htConnPool(const std::string &_ip, uint32_t _port, size_t _size)
{
	m_ip = _ip;
	m_port = _port;
	m_size = _size;
	for (int i = 0; i<m_size; i++) {
		connections.push_back(
			htConnection(ThriftClientPtr( new ThriftClient(m_ip, m_port)) ));
	}
}

htConnPool::htSession htConnPool::get()
{
	hLockTicketPtr get_lock_ticket = get_lock.lock();
	for (int i = 0; i<connections.size(); i++) {
		hLockTicketPtr conn_ticket = connections[i].lock.tryLock();
		if (conn_ticket) {
			return htSession(connections[i].client, conn_ticket);
		}
	}
	htConnection new_conn(ThriftClientPtr( new ThriftClient(m_ip, m_port) ) );
	connections.push_back(new_conn);
	return htSession(new_conn.client, new_conn.lock.tryLock());
}