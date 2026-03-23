# Tomcat File Server — Setup Changes & Maintenance Guide

**Server:** `tganb-db` (`192.168.103.106`)  
**Date:** March 23, 2026  
**Purpose:** Serve ETL files via HTTP using Apache Tomcat

---

## What Was Done (Summary of Changes)

### Root Cause
The systemd `tomcat.service` was running a **different Tomcat instance** than the one being configured.

| Item | Wrong Path (ignored) | Correct Path (running) |
|------|----------------------|------------------------|
| Tomcat Home | `/opt/tomcat/apache-tomcat-9.0.113/` | `/data-drive/etl-process-dev/etl-files/tomcat/` |
| Config Dir | `/opt/tomcat/.../conf/` | `/data-drive/etl-process-dev/etl-files/tomcat/conf/` |
| Context Dir | `/opt/tomcat/.../conf/Catalina/localhost/` | `/data-drive/etl-process-dev/etl-files/tomcat/conf/Catalina/localhost/` |

### Fix Applied
Created a Tomcat context descriptor in the **correct** instance so it serves files from `/data-drive/etl-files/` at the URL path `/files/`.

**File created:**
```
/data-drive/etl-process-dev/etl-files/tomcat/conf/Catalina/localhost/files.xml
```

**Contents:**
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Context docBase="/data-drive/etl-files" path="/files" reloadable="false" />
```

### File Permissions Set
```bash
sudo chmod -R 755 /data-drive/etl-files
sudo chown -R tomcat:tomcat /data-drive/etl-files
```

### Result
Files are now accessible at:
```
http://192.168.103.106:8080/files/<subfolder>/<filename>
```

**Verified working paths:**
- `http://192.168.103.106:8080/files/crimes/57558114-1e5f-4dff-87b1-05bcb66ef2d4.pdf` → 200 ✅
- `http://192.168.103.106:8080/files/chargesheets/40be21c8-f216-411c-8764-9d0ca35466de.pdf` → 200 ✅

---

## Directory Structure

```
/data-drive/etl-files/
├── chargesheets/
├── crimes/
├── fsl_case_property/
├── interrogations/
├── mo_seizures/
├── person/
└── property/
```

---

## Server Maintenance

### 1. Check Tomcat Status
```bash
sudo systemctl status tomcat
```

### 2. Start / Stop / Restart Tomcat
```bash
sudo systemctl start tomcat
sudo systemctl stop tomcat
sudo systemctl restart tomcat
```

### 3. View Live Logs
```bash
# The running Tomcat logs (correct instance)
tail -f /data-drive/etl-process-dev/etl-files/tomcat/logs/catalina.out

# View today's localhost log
tail -f /data-drive/etl-process-dev/etl-files/tomcat/logs/localhost.$(date +%Y-%m-%d).log
```

### 4. Test File Accessibility
```bash
# Test a specific file
curl -I http://192.168.103.106:8080/files/<subfolder>/<filename>

# Quick HTTP status check
curl -s -o /dev/null -w "%{http_code}" http://192.168.103.106:8080/files/crimes/57558114-1e5f-4dff-87b1-05bcb66ef2d4.pdf
```

### 5. Add New Files
Simply copy files into the appropriate subfolder — no Tomcat restart required:
```bash
cp myfile.pdf /data-drive/etl-files/crimes/
# File is immediately available at:
# http://192.168.103.106:8080/files/crimes/myfile.pdf
```

### 6. Add a New Subfolder
```bash
sudo mkdir /data-drive/etl-files/new-folder
sudo chmod 755 /data-drive/etl-files/new-folder
sudo chown tomcat:tomcat /data-drive/etl-files/new-folder
# No restart needed — available immediately at /files/new-folder/
```

### 7. Fix Permission Issues (if files return 403)
```bash
sudo chmod -R 755 /data-drive/etl-files
sudo chown -R tomcat:tomcat /data-drive/etl-files
```

### 8. View / Edit the Context Config
```bash
sudo nano /data-drive/etl-process-dev/etl-files/tomcat/conf/Catalina/localhost/files.xml
```

### 9. Enable Directory Listing (optional)
By default Tomcat returns 404 for folder URLs. To enable browsable directory listing:
```bash
sudo nano /data-drive/etl-process-dev/etl-files/tomcat/conf/web.xml
```
Find the `DefaultServlet` block and set:
```xml
<init-param>
    <param-name>listings</param-name>
    <param-value>true</param-value>
</init-param>
```
Then restart:
```bash
sudo systemctl restart tomcat
```

### 10. Enable Tomcat to Start on Boot
```bash
sudo systemctl enable tomcat
```

### 11. Check Which Tomcat Instance Is Running
```bash
sudo systemctl status tomcat | grep ExecStart
# Should show: /data-drive/etl-process-dev/etl-files/tomcat/bin/startup.sh
```

### 12. Check Open Port
```bash
ss -tlnp | grep 8080
# or
sudo netstat -tlnp | grep 8080
```

---

## Key File Locations

| File | Path |
|------|------|
| Systemd service | `/etc/systemd/system/tomcat.service` |
| Tomcat home | `/data-drive/etl-process-dev/etl-files/tomcat/` |
| Context descriptor | `/data-drive/etl-process-dev/etl-files/tomcat/conf/Catalina/localhost/files.xml` |
| Global context config | `/data-drive/etl-process-dev/etl-files/tomcat/conf/context.xml` |
| Web config | `/data-drive/etl-process-dev/etl-files/tomcat/conf/web.xml` |
| Catalina log | `/data-drive/etl-process-dev/etl-files/tomcat/logs/catalina.out` |
| ETL files root | `/data-drive/etl-files/` |

---

## Notes

- The `/opt/tomcat/apache-tomcat-9.0.113/` installation is **not used** by the systemd service and can be ignored or removed to avoid confusion.
- Tomcat runs on port **8080**.
- Files are served as static content — no application deployment needed for new PDFs or subfolders.
