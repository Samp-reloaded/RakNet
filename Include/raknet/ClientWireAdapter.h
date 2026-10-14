#ifndef RAKNET_CLIENT_WIRE_ADAPTER_H
#define RAKNET_CLIENT_WIRE_ADAPTER_H

namespace RakNet
{
    // Borrowed SDK callbacks. Begin/End run on the owner lane with the peer worker
    // stopped; Decode runs on that worker. Context must outlive the installed adapter.
    struct ClientWireAdapter
    {
        void* context{};
        unsigned short (*begin)(void*, unsigned int, unsigned short) noexcept{};
        void (*end)(void*) noexcept{};
        // Decode in place: zero consumes/rejects; a positive result cannot exceed length.
        int (*decode)(void*, unsigned int, unsigned short, unsigned char*, int) noexcept{};
    };
}

#endif
