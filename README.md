# HaystackAtHome

**HaystackAtHome** is a high-performance, cost-efficient distributed object storage system written in Go. Inspired by Facebook’s **Haystack** architecture, it is specifically optimized for storing and serving massive volumes of photos.

The system is designed for environments where data redundancy and fault tolerance are handled at the hardware or infrastructure level, allowing the software layer to focus entirely on **throughput, low latency, and extreme scalability.**

---

## 🚀 Performance Goals

* **Cost-Efficiency:** Minimizes metadata overhead by packing multiple photos into large physical files.
* **High Throughput:** Optimized for concurrent write-once workloads.
* **Low Latency:** Achieves minimum disk lookups by keeping object offsets in memory.
* **Scale-Out:** Seamlessly add new storage nodes to increase capacity and IOPS.

---

## 🏗️ Architecture

HaystackAtHome consists of two primary services: the **API Gateway (GW)** and the **Storage Service (SS)**. Every storage node runs a single GW instance and one SS instance per physical disk.


### 1. Storage Service (SS)
The SS implements the **Haystack Store** logic. Instead of saving each photo as a separate file—which would overwhelm the filesystem's inode limit and cause excessive disk seeks—it treats the disk as a collection of large **physical volumes**.

* **Needle Packing:** Photos are appended to a volume as "needles." Each needle contains the image data, a unique key, and metadata.
* **In-Memory Index:** Each SS maintains a compact in-memory mapping of `PhotoID -> (Offset, Size)` to ensure that a read request requires exactly one disk seek.
* **No Modifications:** Photos are write-once. Deletions are handled by a "deleted" flag in the needle; the space is eventually reclaimed via copy with compaction to another SS.

### 2. API Gateway (GW)
The GW acts as the intelligent entry point for all client requests.

* **Smart Balancing:** Uploads are routed to Storage Services based on real-time **IO stats** (latency/throughput) and available disk capacity.
* **Object Mapping:** Maintains the directory of which storage nodes/disks hold specific photo IDs.
* **Hot-Object Caching:** Features an LRU in-memory cache to serve frequently accessed photos without hitting the disk.
* **Security:** Integrated Authentication and Authorization layer for every request.

---

## 📊 Workload Assumptions

HaystackAtHome is tuned for a specific, photo-heavy traffic pattern:
* **Read-Heavy:** 5–10x more reads than uploads.
* **Write-Once:** Objects are never modified; they are only uploaded or deleted.
* **Deletion Rate:** Low (approximately 0.2 deletions for every upload).
* **Infrastructure:** Assumes fault-tolerant storage nodes/disks (e.g., RAID or high-reliability cloud block storage).

---

## 🛠️ Tech Stack

* **Language:** Go (Golang) for high concurrency and efficient memory management.
* **Communication:** gRPC for internal service communication; REST for the public API.
* **Caching:** In-memory LRU implementation.

---

## 🏁 Getting Started

### Prerequisites
* Go 1.21+
* Fast SSDs (Recommended for SS disks)

### Installation
```bash
git clone https://github.com/ShortestStraw/HaystackAtHome.git
cd HaystackAtHome
make build
```

### Running a Storage Node
To start a Storage Service for a specific disk:
```bash
./ss -config config.toml -name <ss-name>
```

To start the API Gateway:
```bash
./gw -config config.toml -name <gw-name>
```