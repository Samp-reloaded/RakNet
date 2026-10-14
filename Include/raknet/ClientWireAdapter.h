#pragma once
// NOTSA: real RakNet SDK boundary; callbacks are installed before the worker starts.

namespace RakNet {
class ClientWireAdapter final {
public:
    using BeginCallback = unsigned short (*)(void*, unsigned int, unsigned short) noexcept;
    using EndCallback = void (*)(void*) noexcept;
    using DecodeCallback = int (*)(void*, unsigned int, unsigned short, unsigned char*, int) noexcept;

    constexpr ClientWireAdapter() noexcept = default;
    constexpr ClientWireAdapter(void* context, BeginCallback begin,
        EndCallback end, DecodeCallback decode) noexcept
        : m_context{context}, m_begin{begin}, m_end{end}, m_decode{decode} {}
    [[nodiscard]] unsigned short Begin(unsigned int address, unsigned short port) const noexcept {
        return m_begin ? m_begin(m_context, address, port) : 0x6969u;
    }
    void End() const noexcept {
        if (m_end) m_end(m_context);
    }
    // Zero consumes/rejects; a positive result is the checked in-place plaintext size.
    [[nodiscard]] int Decode(unsigned int address, unsigned short port,
        unsigned char* data, int length) const noexcept {
        return m_decode ? m_decode(m_context, address, port, data, length) : length;
    }
private:
    void* m_context{};
    BeginCallback m_begin{};
    EndCallback m_end{};
    DecodeCallback m_decode{};
};
}
