import os
import re
from pathlib import Path

def scan_env_vars():
    # usage: { var_name: [ { 'file': path, 'default': val, 'line': content } ] }
    env_usage = {}
    base_dir = Path('.')
    
    # Regex patterns to capture variable name and optional default value
    patterns = [
        (r"os\.getenv\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*(['\"].*?['\"]|[^)]+))?\s*\)", "python"),
        (r"os\.environ\.get\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*(['\"].*?['\"]|[^)]+))?\s*\)", "python"),
        (r"os\.environ\[\s*['\"]([^'\"]+)['\"]\s*\]", "python"),
        (r"config\(\s*['\"]([^'\"]+)['\"](?:\s*,\s*(['\"].*?['\"]|[^)]+))?\s*\)", "python"),
        (r"process\.env\.([a-zA-Z0-9_]+)", "js"),
        (r"process\.env\[\s*['\"]([^'\"]+)['\"]\s*\]", "js")
    ]
    
    for path in base_dir.rglob('*'):
        if any(part in str(path) for part in ['venv', 'node_modules', '.git', '__pycache__']):
            continue
        if path.suffix not in ['.py', '.js', '.ts', '.sh', '.sql']:
            continue
            
        try:
            content = path.read_text(encoding='utf-8', errors='ignore')
            lines = content.splitlines()
            for line_idx, line in enumerate(lines):
                for pattern, lang in patterns:
                    matches = re.finditer(pattern, line)
                    for match in matches:
                        var_name = match.group(1)
                        # Avoid interpolation variables like {var}
                        if var_name.startswith('{') or var_name.endswith('}'):
                            continue
                            
                        default_val = None
                        if lang == "python" and len(match.groups()) > 1:
                            default_val = match.group(2)
                        
                        if var_name not in env_usage:
                            env_usage[var_name] = []
                        
                        env_usage[var_name].append({
                            'file': str(path),
                            'line_num': line_idx + 1,
                            'default': default_val,
                            'context': line.strip()
                        })
        except Exception:
            pass
            
    return env_usage

def parse_env_file(env_path):
    env_vars = {}
    if not env_path.exists():
        return env_vars
        
    content = env_path.read_text(encoding='utf-8', errors='ignore')
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            key, value = line.split('=', 1)
            env_vars[key.strip()] = value.strip()
    return env_vars

def validate():
    usage = scan_env_vars()
    env_file = Path('.env')
    present_vars = parse_env_file(env_file)
    
    missing = {}
    empty = {}
    valid = {}
    
    for var in sorted(usage.keys()):
        if var not in present_vars:
            missing[var] = usage[var]
        elif not present_vars[var]:
            empty[var] = usage[var]
        else:
            valid[var] = usage[var]
            
    print("--- ENVIRONMENT VARIABLE VALIDATION REPORT ---")
    
    print(f"\n[PRESENT] Present and valid variables ({len(valid)})")
    
    print(f"\n[MISSING] Missing variables ({len(missing)})")
    for var, contexts in missing.items():
        # Heuristic: if all contexts have a default, it's less critical
        has_default = all(c['default'] is not None for c in contexts)
        status = " (Has Default)" if has_default else " (NO DEFAULT)"
        print(f"  {var}{status}")
        unique_files = sorted(list(set(c['file'] for c in contexts)))
        for f in unique_files[:3]: # limit to 3 files
            print(f"    - {f}")
        if len(unique_files) > 3:
            print(f"    - ... and {len(unique_files)-3} more")
            
    print(f"\n[EMPTY] Variables present but empty ({len(empty)})")
    for var, contexts in empty.items():
        print(f"  {var}")
        unique_files = sorted(list(set(c['file'] for c in contexts)))
        for f in unique_files[:3]:
            print(f"    - {f}")

if __name__ == '__main__':
    validate()

if __name__ == '__main__':
    validate()
