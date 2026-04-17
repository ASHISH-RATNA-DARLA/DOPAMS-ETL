# NFS Shared Storage Setup for ETL and Tomcat Servers

## Overview

This document describes the configuration implemented to allow the **ETL server** to store files directly on the **Tomcat server's storage** using **NFS (Network File System)**.

This architecture ensures that:

* The **ETL server downloads and writes files**
* Files are **stored physically on the Tomcat server**
* **Tomcat can immediately serve those files to the frontend**

---

# Architecture

```
           +--------------------------------+
           |        ETL SERVER              |
           |         (dopams)               |
           |      192.168.103.182           |
           |                                |
           |   /mnt/shared-etl-files        |
           +---------------+----------------+
                           |
                           | NFS
                           |
                           v
           +--------------------------------+
           |        TOMCAT SERVER           |
           |        192.168.103.106         |
           |                                |
           |      /data-drive/etl-files     |
           |                                |
           |  Tomcat serves files via       |
           |  http://192.168.103.106/files  |
           +---------------+----------------+
                           |
                           v
                    Frontend Clients
```

---

# Servers Involved

| Server              | Role                | User    |
| ------------------- | ------------------- | ------- |
| **192.168.103.106** | Tomcat + NFS Server | `tganb` |
| **192.168.103.182** | ETL Server (dopams) | `eagle` |

---

# Step 1 — Create Shared Storage on Tomcat Server

Login to **Server 106**.

```
ssh tganb@192.168.103.106
```

Create a directory that will store ETL files.

```bash
sudo mkdir -p /data-drive/etl-files
```

Set permissions so ETL can write files.

```bash
sudo chmod -R 777 /data-drive/etl-files
```

---

# Step 2 — Install NFS Server

```bash
sudo apt update
sudo apt install nfs-kernel-server -y
```

---

# Step 3 — Configure NFS Export

Edit the exports file.

```bash
sudo nano /etc/exports
```

Add the following line:

```
/data-drive/etl-files 192.168.103.0/24(rw,sync,no_subtree_check,no_root_squash) 192.168.102.0/24(rw,sync,no_subtree_check,no_root_squash)
```

This allows machines from both LAN networks to access the shared storage:

* **192.168.103.x**
* **192.168.102.x**

---

# Step 4 — Restart NFS Server

Apply the configuration.

```bash
sudo exportfs -a
sudo systemctl restart nfs-kernel-server
```

Verify export:

```bash
sudo exportfs -v
```

Example output:

```
/data-drive/etl-files
192.168.103.0/24(rw,sync,no_subtree_check)
192.168.102.0/24(rw,sync,no_subtree_check)
```

---

# Step 5 — Configure ETL Server (dopams)

Login to the **ETL server**.

```
ssh eagle@192.168.103.182
```

Install NFS client tools.

```bash
sudo apt update
sudo apt install nfs-common -y
```

---

# Step 6 — Create Mount Directory

Create a local mount directory.

```bash
sudo mkdir -p /mnt/shared-etl-files
```

---

# Step 7 — Mount the Shared Storage

Mount the NFS share.

```bash
sudo mount 192.168.103.106:/data-drive/etl-files /mnt/shared-etl-files
```

Verify the mount.

```bash
df -h
```

Example output:

```
192.168.103.106:/data-drive/etl-files
mounted on /mnt/shared-etl-files
```

---

# Step 8 — Test File Sharing

On the **ETL server**:

```bash
touch /mnt/shared-etl-files/test_file.txt
```

On the **Tomcat server**:

```bash
ls /data-drive/etl-files
```

Expected output:

```
test_file.txt
```

This confirms that the **ETL server can write files to the Tomcat server storage**.

---

# Step 9 — Make Mount Persistent

To ensure the mount survives server reboot, modify `/etc/fstab` on the **ETL server**.

```bash
sudo nano /etc/fstab
```

Add the following line:

```
192.168.103.106:/data-drive/etl-files /mnt/shared-etl-files nfs defaults,_netdev 0 0
```

Apply the configuration.

```bash
sudo mount -a
```

If no errors appear, the configuration is correct.

---

# Step 10 — ETL Configuration

Update the ETL `.env` configuration.

```
FILES_MEDIA_BASE_PATH=/mnt/shared-etl-files
FILES_BASE_URL=http://192.168.103.106:8080/files
```

---

# File Flow

1. ETL downloads a file from an API
2. ETL saves the file to

```
/mnt/shared-etl-files/<file>.pdf
```

3. Because this path is an **NFS mount**, the file is physically stored on

```
/data-drive/etl-files/<file>.pdf
```

4. Tomcat serves the file via

```
http://192.168.103.106:8080/files/<file>.pdf
```

---

# Benefits of This Architecture

* Centralized storage
* ETL and Tomcat run on separate servers
* Immediate file availability for Tomcat
* Large shared storage capacity
* Simple and reliable network filesystem
* Easily scalable for additional internal servers

---

# Final Result

The system now uses a **shared network filesystem** so that:

* **ETL writes files**
* **Tomcat serves them**
* **Frontend can access them immediately**

This is a common **enterprise architecture pattern used in distributed ETL pipelines**.
