/* 
 * File:   htConnPool.h
 * Author: phrk
 *
 * Created on December 21, 2013, 12:26 PM
 */

#ifndef HTCONNPOOL_H
#define	HTCONNPOOL_H

#include "htDba.h"
#include "../tetramorph/threadpool/locks.h"

class htConnPool
{
public:
	class htSession
	{
	public:
		ThriftClientPtr client;
		hLockTicketPtr lock_ticket;
		
		htSession(ThriftClientPtr _client,
				hLockTicketPtr _lock_ticket);
	};
	
private:
	
	class htConnection
	{
	public:
		ThriftClientPtr client;
		hAutoLock lock;
		
		htConnection(ThriftClientPtr _client);
	};
	
	std::string m_ip;
	uint32_t m_port;
	size_t m_size;
	
	std::vector<htConnection> connections;
	hAutoLock get_lock;
public:
	htConnPool(const std::string &_ip, uint32_t _port, size_t _size);
	htSession get();
};

typedef boost::shared_ptr<htConnPool> htConnPoolPtr;

#endif	/* HTCONNPOOL_H */

