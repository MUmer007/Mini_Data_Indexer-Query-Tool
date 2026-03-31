# 🚀 Mini Data Indexer and Query Tool

A high-performance, data-structures-driven Python project designed to efficiently process large CSV datasets, build optimized indices, and benchmark naive vs advanced query strategies.

---

## 🔥 Key Highlights

- ⚡ Built efficient indexing systems from scratch  
- 📊 Benchmarked naive vs optimized algorithms  
- 🧠 Applied core data structures (hash maps, heaps, sets, binary search)  
- 🏎️ Achieved up to 1900× speed improvement  
- 🧪 Designed a reproducible benchmarking pipeline  
- 🛠️ Implemented a CLI-based query engine  

---

## 🏗️ Project Architecture

project/
├── main.py              
├── generator.py         
├── loader.py            
├── queries.py           
├── experiments.py       
├── dataset_small.csv    
├── dataset_spec.txt     
├── analysis.pdf         
├── README.md            
└── results/

---

## ⚙️ Technologies & Tools Used

### 🧑‍💻 Core Technologies
- Python 3.10+
- Standard Library Only

### 🧠 Concepts & Techniques
- Hash Maps, Sets, Heaps
- Binary Search
- Sorting Algorithms
- Time Complexity Optimization
- time.perf_counter()

### 🤖 AI Tools Used
- Claude → Code generation & structuring  
- GPT (ChatGPT) → Prompting & documentation  

---

## 🚀 Quick Start

### Generate Dataset
python generator.py --size 100000 --seed 42 --output dataset_main.csv

### Run Queries
python main.py --build dataset_main.csv --lookup 1000123

### Run Benchmarks
python experiments.py --dataset dataset_main.csv

---

## 📊 Performance Results

- ID Lookup: ~1900× faster  
- Top-K Query: ~3× faster  
- Range Query: ~22× faster  

---

## 🧠 Learning Outcomes

- Strong understanding of DSA  
- Performance optimization mindset  
- System design fundamentals  

---

## 🎯 Design Principles

- Separation of Concerns  
- Clean OOP Design  
- Reproducibility  
- Performance First  
- Zero Dependencies  

---

## 👨‍💻 Author

Muhammad Umer

---

## ⭐ Final Note

This project demonstrates how to think like an engineer, not just a coder.
