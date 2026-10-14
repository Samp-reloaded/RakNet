#pragma once

#include <cstddef>
#include <limits>

#include "NetworkTypes.h"

namespace RakNet
{
	// a caller shares one ledger across its complete owner-lane slice.
	enum class ReceiveStatus : unsigned int { Packet, Empty, Yield, Failure };
	enum class ReceiveFailure : unsigned int { None, InvalidBudget, OversizedPacket, MalformedPacket, ReentrantReceive, WorkerStopped };

	class ReceiveResult
	{
	public:
		[[nodiscard]] static ReceiveResult PacketReceived(Packet& packet) { return {ReceiveStatus::Packet, &packet, ReceiveFailure::None}; }
		[[nodiscard]] static ReceiveResult Empty() { return {ReceiveStatus::Empty, nullptr, ReceiveFailure::None}; }
		[[nodiscard]] static ReceiveResult Yielded() { return {ReceiveStatus::Yield, nullptr, ReceiveFailure::None}; }
		[[nodiscard]] static ReceiveResult Failed(ReceiveFailure failure) { return {ReceiveStatus::Failure, nullptr, failure}; }
		[[nodiscard]] ReceiveStatus GetStatus() const { return status; }
		// SDK ownership transfers only on Packet; release through DeallocatePacket.
		[[nodiscard]] Packet* GetPacket() const { return packet; }
		[[nodiscard]] ReceiveFailure GetFailure() const { return failure; }

	private:
		ReceiveResult(ReceiveStatus status, Packet* packet, ReceiveFailure failure) : status(status), packet(packet), failure(failure) {}
		ReceiveStatus status;
		Packet* packet;
		ReceiveFailure failure;
	};

	class ReceiveBudget
	{
	public:
		ReceiveBudget(std::size_t packets, std::size_t bytes, std::size_t workUnits)
			: packetLimit(packets), byteLimit(bytes), workLimit(workUnits) {}
		[[nodiscard]] std::size_t GetProcessedPackets() const { return processedPackets; }
		[[nodiscard]] std::size_t GetProcessedBytes() const { return processedBytes; }
		[[nodiscard]] std::size_t GetWorkUnits() const { return workUnits; }
		[[nodiscard]] std::size_t GetQueueTransfers() const { return queueTransfers; }
		[[nodiscard]] std::size_t GetDiscardedPackets() const { return discardedPackets; }
		[[nodiscard]] std::size_t GetDiscardedBytes() const { return discardedBytes; }

	private:
		friend class RakPeer;
		[[nodiscard]] static ReceiveBudget Unlimited()
		{
			constexpr auto unlimited = std::numeric_limits<std::size_t>::max();
			ReceiveBudget budget{unlimited, unlimited, unlimited};
			budget.compatibilityMode = true;
			return budget;
		}
		[[nodiscard]] bool IsValid() const { return packetLimit > 0 && byteLimit > 0 && workLimit > 0; }
		[[nodiscard]] bool CanChargeWork(std::size_t count) const { return count <= workLimit - workUnits; }
		[[nodiscard]] bool CanAdmit(std::size_t bytes, std::size_t packetWork) const
		{
			return processedPackets < packetLimit && bytes <= byteLimit - processedBytes && CanChargeWork(packetWork);
		}
		void ChargePacket(std::size_t bytes, std::size_t packetWork)
		{
			++processedPackets;
			processedBytes += bytes;
			workUnits += packetWork;
		}
		void ChargeTransfer() { ++queueTransfers; ++workUnits; }
		std::size_t packetLimit;
		std::size_t byteLimit;
		std::size_t workLimit;
		std::size_t processedPackets{};
		std::size_t processedBytes{};
		std::size_t workUnits{};
		std::size_t queueTransfers{};
		std::size_t discardedPackets{};
		std::size_t discardedBytes{};
		bool pluginsUpdated{};
		bool compatibilityMode{};
	};
}
