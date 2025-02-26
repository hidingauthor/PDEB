# Pushdown Execution Blocks (PDEB) Framework  

This repository contains the implementation of the **Pushdown Execution Blocks (PDEB)** framework for optimizing imperative programs in **Multiple Database Systems (MDBS)**. PDEB enhances traditional optimization techniques by **identifying and fragmenting execution blocks (EBs)**, allowing for **distributed optimization** across Component Databases (CDBs).  

## Repository Structure  
The repository is organized as follows:  

PDEB/ │── src/ # Source code for the PDEB framework
│ ├── AugmentGraph.py # Parses Intermediate Representation (IR) for demonstration. 
│ ├── ForwardGreedyApproach.py # Implements the cost-based Sink operator. It implements logical planning stage
│ ├── ProgramWriter.py # Implements the ProgramRewriter operator. It implements fragmentation & decomposition stage.
│── workloads/ # Benchmark workload programs
│ ├── ES-SOAP/ # Extended Structure of SOAP benchmark
│ ├── P-TPC-DS/ # Procedural TPC-DS benchmark
│ ├── R-TPC-E/ # Read-only TPC-E benchmark
│── results/ # Experimental results
│── scripts/ # Helper scripts for setting up experiments
│── README.md # Instructions on setup and reproduction

## **Implementation Overview**  
We implement PDEB in Python, using **SAP HANA (SDA) and PostgreSQL (FDW)** as MDBS environments. The PDEB framework consists of two main **optimization operators**:  

1. **Sink Operator:** Identifies beneficial **remote execution blocks (EBs)** by pushing down imperative constructs based on a cost model.  
2. **ProgramRewriter Operator:** **Fragments the program** into modular execution blocks and **decomposes** it into a **distributed execution plan** for efficient execution.  

The **workflow of PDEB** follows these key stages:  

### **1️. Logical Planning Stage (Sink Operator)**  
- Parses the **Intermediate Representation (IR)** using **AugmentGraph.py**.  
- Identifies **declarative EBs** (leaf statements mapped to specific CDBs) using **ForwardGreedyApproach.py**.  
- Enhances declarative EBs with **imperative constructs** if beneficial using **ForwardGreedyApproach.py**.  
- Applies a **cost-based Sink Operator** to relocate imperative constructs to candidate CDBs using **ForwardGreedyApproach.py**.  

### **2️. Fragmentation & Decomposition Stage (ProgramRewriter Operator)**  
- Fragments execution blocks into self-contained modular units using **ProgramWriter.py**.  
- Resolves producer-consumer relationships between execution blocks.  
- Decomposes logical execution plans into **distributed physical execution plans**.  
- Assigns execution blocks to **specific CDBs** for distributed optimization.  

---

## **Reproducing Our Experiments**  
We provide three benchmark workloads for evaluation:  

1. **ES-SOAP (Extended Structure of SOAP)**  
   - Includes **imperative SQL functions** with multiple producer-consumer dependencies.  
   - Represents **nested loops, branch conditions, and procedural structures**.  

2. **P-TPC-DS (Procedural TPC-DS Queries)**  
   - Adapts **TPC-DS queries** to include procedural constructs.  
   - Simulates **realistic OLAP workloads** in MDBS.  

3. **R-TPC-E (Read-Only TPC-E Programs)**  
   - Extracts **read-only OLTP queries** from the **TPC-E benchmark**.  
   - Evaluates **CDB variation** and **data federation impact**.  

### **Setup Environment**  
#### **Prerequisites**  
- Python 3.8+  
- SAP HANA SDA (Smart Data Access)  
- PostgreSQL FDW (Foreign Data Wrapper)  
- Dependencies: Install required Python packages  

####Configure MDBS Connections########## in FederationConnector.py

{
    "CDB_1": {
        "db_type": "SAP_HANA",
        "host": "hana_host",
        "port": "30015",
        "user": "admin",
        "password": "password"
    },
    "CDB_2": {
        "db_type": "PostgreSQL",
        "host": "pg_host",
        "port": "5432",
        "user": "postgres",
        "password": "password"
    }
}

####### Run PDEB Optimization ##########
please run ###main.py#######
in the function #####using_algorithm("FG", ag, fc, pfd)##### proposed PDEB#########
in the function #####using_algorithm("ORG", ag, fc, pfd)##### traditional #########

########## Results & Observations #############
PDEB reduces execution latency by up to 21X in OLAP and 1.5X in OLTP.
