/// \file
/// \brief \b [Internal] Passes queued data between threads using a circular buffer with read and write pointers
///
/// This file is part of RakNet Copyright 2003 Kevin Jenkins.
///
/// Usage of RakNet is subject to the appropriate license agreement.
/// Creative Commons Licensees are subject to the
/// license found at
/// http://creativecommons.org/licenses/by-nc/2.5/
/// Single application licensees are subject to the license found at
/// http://www.rakkarsoft.com/SingleApplicationLicense.html
/// Custom license users are subject to the terms therein.
/// GPL license users are subject to the GNU General Public
/// License as published by the Free
/// Software Foundation; either version 2 of the License, or (at your
/// option) any later version.

#ifndef __SINGLE_PRODUCER_CONSUMER_H
#define __SINGLE_PRODUCER_CONSUMER_H

#include <atomic>
#include <cstddef>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "Export.h"
#include "RakAssert.h"

namespace RakNet::DataStructures
{
	// Cursors belong exclusively to their endpoint. Only slot state, links and
	// observation counters cross threads; no node is deleted while either runs.
	template <class SingleProducerConsumerType>
	class RAK_DLL_EXPORT SingleProducerConsumer
	{
	public:
		SingleProducerConsumer();
		~SingleProducerConsumer() = default;
		SingleProducerConsumer(const SingleProducerConsumer&) = delete;
		SingleProducerConsumer& operator=(const SingleProducerConsumer&) = delete;

		// Locks may be taken ahead, then unlocked in their original FIFO order.
		[[nodiscard]] SingleProducerConsumerType* WriteLock();
		void CancelWriteLock(SingleProducerConsumerType* cancelToLocation);
		void WriteUnlock();
		[[nodiscard]] SingleProducerConsumerType* ReadLock();
		void CancelReadLock(SingleProducerConsumerType* cancelToLocation);
		void ReadUnlock();

		// Both endpoints must be quiescent for Clear and destruction.
		void Clear();
		// Legacy int boundary, saturated; observation only, never a queue barrier.
		[[nodiscard]] int Size() const;
		[[nodiscard]] bool CheckReadUnlockOrder(const SingleProducerConsumerType* data) const;
		[[nodiscard]] bool ReadIsLocked() const;

	private:
		enum class SlotState : unsigned int { Free, WriteReserved, Published };
		struct DataPlusPtr
		{
			SingleProducerConsumerType object{};
			std::atomic<SlotState> state{SlotState::Free};
			std::atomic<DataPlusPtr*> next{};
		};
		static constexpr std::size_t MinimumListSize = 8;
		std::vector<std::unique_ptr<DataPlusPtr>> ownedNodes;
		DataPlusPtr* readAheadPointer{};
		DataPlusPtr* writeAheadPointer{};
		DataPlusPtr* readPointer{};
		DataPlusPtr* writePointer{};
		std::atomic<std::size_t> readCount{};
		std::atomic<std::size_t> writeCount{};
	};

	template <class T>
	SingleProducerConsumer<T>::SingleProducerConsumer()
	{
		ownedNodes.reserve(MinimumListSize);
		for (std::size_t index = 0; index < MinimumListSize; ++index)
			ownedNodes.push_back(std::make_unique<DataPlusPtr>());
		Clear();
	}

	template <class T>
	T* SingleProducerConsumer<T>::WriteLock()
	{
		DataPlusPtr* slot = writeAheadPointer;
		// Acquire the consumer's release before reusing its object storage.
		const auto state = slot->state.load(std::memory_order_acquire);
		RakAssert(state == SlotState::Free);
		static_cast<void>(state);
		DataPlusPtr* next = slot->next.load(std::memory_order_acquire);
		if (next->state.load(std::memory_order_acquire) != SlotState::Free)
		{
			// Keep one free sentinel after all write reservations. Growth never
			// changes a published/read-locked node, and pointees never relocate.
			auto inserted = std::make_unique<DataPlusPtr>();
			inserted->next.store(next, std::memory_order_relaxed);
			DataPlusPtr* insertedSlot = inserted.get();
			ownedNodes.push_back(std::move(inserted));
			slot->next.store(insertedSlot, std::memory_order_release);
			next = insertedSlot;
		}
		slot->state.store(SlotState::WriteReserved, std::memory_order_relaxed);
		writeAheadPointer = next;
		return &slot->object;
	}

	template <class T>
	void SingleProducerConsumer<T>::CancelWriteLock(T* cancelToLocation)
	{
		DataPlusPtr* canceled = writePointer;
		while (canceled != writeAheadPointer && &canceled->object != cancelToLocation)
			canceled = canceled->next.load(std::memory_order_relaxed);
		RakAssert(canceled != writeAheadPointer);
		if (canceled == writeAheadPointer)
			return;
		DataPlusPtr* slot = canceled;
		while (slot != writeAheadPointer)
		{
			slot->state.store(SlotState::Free, std::memory_order_relaxed);
			slot = slot->next.load(std::memory_order_relaxed);
		}
		writeAheadPointer = canceled;
	}

	template <class T>
	void SingleProducerConsumer<T>::WriteUnlock()
	{
		DataPlusPtr* slot = writePointer;
		RakAssert(slot != writeAheadPointer);
		RakAssert(slot->state.load(std::memory_order_relaxed) == SlotState::WriteReserved);
		DataPlusPtr* next = slot->next.load(std::memory_order_relaxed);
		writeCount.fetch_add(1, std::memory_order_relaxed);
		// This publishes the payload, its pointee initialization and next link.
		slot->state.store(SlotState::Published, std::memory_order_release);
		writePointer = next;
	}

	template <class T>
	T* SingleProducerConsumer<T>::ReadLock()
	{
		DataPlusPtr* slot = readAheadPointer;
		if (slot->state.load(std::memory_order_acquire) != SlotState::Published)
			return nullptr;
		// The publication acquire precedes every object/pointee and link read.
		readAheadPointer = slot->next.load(std::memory_order_acquire);
		return &slot->object;
	}

	template <class T>
	void SingleProducerConsumer<T>::CancelReadLock(T* cancelToLocation)
	{
		DataPlusPtr* canceled = readPointer;
		while (canceled != readAheadPointer && &canceled->object != cancelToLocation)
			canceled = canceled->next.load(std::memory_order_relaxed);
		RakAssert(canceled != readAheadPointer);
		if (canceled != readAheadPointer)
			readAheadPointer = canceled;
	}

	template <class T>
	void SingleProducerConsumer<T>::ReadUnlock()
	{
		DataPlusPtr* slot = readPointer;
		RakAssert(slot != readAheadPointer);
		RakAssert(slot->state.load(std::memory_order_relaxed) == SlotState::Published);
		// Capture the link and advance BEFORE the producer may reuse this node.
		readPointer = slot->next.load(std::memory_order_acquire);
		readCount.fetch_add(1, std::memory_order_release);
		slot->state.store(SlotState::Free, std::memory_order_release);
	}

	template <class T>
	void SingleProducerConsumer<T>::Clear()
	{
		ownedNodes.resize(MinimumListSize);
		for (std::size_t index = 0; index < MinimumListSize; ++index)
		{
			ownedNodes[index]->state.store(SlotState::Free, std::memory_order_relaxed);
			ownedNodes[index]->next.store(ownedNodes[(index + 1) % MinimumListSize].get(), std::memory_order_relaxed);
		}
		readPointer = ownedNodes.front().get();
		writePointer = readPointer;
		readAheadPointer = readPointer;
		writeAheadPointer = readPointer;
		readCount.store(0, std::memory_order_relaxed);
		writeCount.store(0, std::memory_order_relaxed);
	}

	template <class T>
	int SingleProducerConsumer<T>::Size() const
	{
		const auto consumed = readCount.load(std::memory_order_acquire);
		const auto published = writeCount.load(std::memory_order_relaxed);
		const auto count = published - consumed;
		constexpr auto maximum = static_cast<std::size_t>(std::numeric_limits<int>::max());
		return static_cast<int>(count > maximum ? maximum : count);
	}

	template <class T>
	bool SingleProducerConsumer<T>::CheckReadUnlockOrder(const T* data) const
	{
		return &readPointer->object == data;
	}

	template <class T>
	bool SingleProducerConsumer<T>::ReadIsLocked() const
	{
		return readAheadPointer != readPointer;
	}
}

#endif
