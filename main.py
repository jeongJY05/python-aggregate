import argparse
import requests
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict

def parse_args():
  parser = argparse.ArgumentParser(description="Aggregate log file data.")
  parser.add_argument('--url', required=True, help='Log file URL')
  parser.add_argument('--date', required=True, help='Target date for aggregation (YYYYMMDD)')
  parser.add_argument('--active', choices=['on', 'off'], default='on', help='Toggle ACTIVE row output')
  parser.add_argument('--state', choices=['on', 'off'], default='on', help='Toggle STATE column output')
  parser.add_argument('--action', choices=['on', 'off'], default='on', help='Toggle ACTION column output')
  return parser.parse_args()

def download_log_file(url):
  response = requests.get(url)
  response.raise_for_status()
  return response.text.splitlines()

def gmt_to_jst(gmt_time_str):
  gmt_time_str = gmt_time_str.strip('[]')
  gmt_time = datetime.strptime(gmt_time_str, '%Y-%m-%dT%H:%M:%S.%fZ')
  jst_time = gmt_time + timedelta(hours=9)
  return jst_time

def parse_logs(log_lines, target_date):
  # 型：dictionary
  #   SessionId
  #   log_date（ログの日時）, list<ログデータ>
  sessions = defaultdict(list)
  for line in log_lines:
    line = line.strip()

    if not line:
        continue

    ## 日付・ログ区分：TAB
    parts = line.split('\t', 1)

    ## 形式に合わないログがあれば外す
    if len(parts) != 2:
        continue

    # parts[0] = date_str, YYYY-MM-DDTHH:MI:SS:sssZ
    # 　ex) [2021-07-25T21:46:39.549Z]
    # parts[1] = log_str, json形式 
    # 　ex) {"logType":"startSession","vhaId":"115","sessionId":"722972664639320","sessionIsValid":"0","sessionSubId":"722972664639320_0","characterId":"rachel"}
    date_str, log_str = parts

    # JSTに変換して日付比較
    log_date = gmt_to_jst(date_str)
    if log_date.strftime('%Y%m%d') != target_date:
        continue
    try:
        log_entry = json.loads(log_str)
    except json.JSONDecodeError:
        continue

    session_id = log_entry.get('sessionId')
    sessions[session_id].append((log_date, log_entry))
  return sessions

def calculate_aggregate(sessions):
  # (1). sessionIdのユニーク数
  all_count = len(sessions)

  total_session_duration = 0
  # (3). logTypeの値が、"changeState"の数
  change_state_count = 0
  # (5). logTypeの値が、"userAction"の数
  user_action_count = 0
  valid_sessions = defaultdict(list)
  
  for session_id, logs in sessions.items():
    break;
    min_time = min(logs, key=lambda x: x[0])[0]
    max_time = max(logs, key=lambda x: x[0])[0]
    session_duration = (max_time - min_time).total_seconds()
    total_session_duration += session_duration

    for log_date, log_entry in logs:
      if log_entry.get('logType') == 'changeState':
        change_state_count += 1
      if log_entry.get('logType') == 'userAction':
        user_action_count += 1
      if log_entry.get('sessionIsValid') == '1':
        valid_sessions[session_id].append((log_date, log_entry))
  
  # (2). sessionIdで、(最大時間 - 最小時間) / (1)の値
  all_duration_avg = total_session_duration / all_count if all_count else 0
  # (4). (3)の値 / (1)の値
  all_change_state_avg = change_state_count / all_count if all_count else 0
  # (6). (5)の値 / (1)の値
  all_user_action_avg = user_action_count / all_count if all_count else 0

  # (7). sessionIsValid=1のsessionIdのユニーク数
  valid_session_count = len(valid_sessions)
  # (9). sessionIsValid=1で、logTypeの値が"changeState"の数
  valid_change_state_count = 0
  # (11). sessionIsValid=1で、logTypeの値が"userAction"の数
  valid_user_action_count = 0
  valid_duration_sum = 0

  for session_id, logs in valid_sessions.items():
    min_time = min(logs, key=lambda x: x[0])[0]
    start_valid_time = next((x[0] for x in logs if x[1].get('logType') == 'startValidSession'), min_time)
    max_time = max(logs, key=lambda x: x[0])[0]
    valid_duration_sum += (max_time - start_valid_time).total_seconds()
    
    for log_date, log_entry in logs:
      if log_entry.get('logType') == 'changeState':
        valid_change_state_count += 1
      if log_entry.get('logType') == 'userAction':
        valid_user_action_count += 1

  # (8). sessionIsValid=1の、(最大時間 - logTypeの値がstartValidSessionの時間) / (7)の値 を秒で算出
  valid_duration_avg = valid_duration_sum / valid_session_count if valid_session_count else 0
  # (10). (9)の値 / (7)の値
  valid_change_state_avg = valid_change_state_count / valid_session_count if valid_session_count else 0
  # (12). (11)の値 / (7)の値
  valid_user_action_avg = valid_user_action_count / valid_session_count if valid_session_count else 0
  
  return (all_count, all_duration_avg, change_state_count, all_change_state_avg, user_action_count, all_user_action_avg,
        	valid_session_count, valid_duration_avg, valid_change_state_count, valid_change_state_avg,
        	valid_user_action_count, valid_user_action_avg)

def print_results(results, elapsed_time, active_on, state_on, action_on):
  (all_count, all_duration, change_state_count, change_state_avg, user_action_count, user_action_avg, valid_count, valid_duration_avg, valid_change_state_count, valid_change_state_avg, valid_user_action_count, valid_user_action_avg) = results

  header = "+----------+---------------------+"
  header += "--------+--------+" if state_on == 'on' else ""
  header += "--------+--------+" if action_on == 'on' else ""
  header += "\n"
  
  sub_header = "|          | SESSION             |"
  sub_header += " STATE           |" if state_on == 'on' else ""
  sub_header += " ACTION          |" if action_on == 'on' else ""
  sub_header += "\n"
  
  header_line = "+          +---------------------+"
  header_line += "--------+--------+" if state_on == 'on' else ""
  header_line += "--------+--------+" if action_on == 'on' else ""
  header_line += "\n"
  
  columns = "|          | COUNT   | TIME(sec) |"
  columns += " COUNT  | AVG    |" if state_on == 'on' else ""
  columns += " COUNT  | AVG    |" if action_on == 'on' else ""
  columns += "\n"
  
  line = "+----------+---------------------+"
  line += "--------+--------+" if state_on == 'on' else ""
  line += "--------+--------+" if action_on == 'on' else ""
  line += "\n"

  all_row = f"| ALL      | {all_count:<7} | {all_duration:<9.1f} |"
  
  if state_on == 'on':
    all_row += f" {change_state_count:<6} | {change_state_avg:<6.1f} |"
  if action_on == 'on':
    all_row += f" {user_action_count:<6} | {user_action_avg:<6.1f} |"
  all_row += "\n"
  active_row = ""
  
  if active_on == 'on':
    active_row = f"| ACTIVE   | {valid_count:<7} | {valid_duration_avg:<9.1f} |"
    if state_on == 'on':
      active_row += f" {valid_change_state_count:<6} | {valid_change_state_avg:<6.1f} |"
    if action_on == 'on':
      active_row += f" {valid_user_action_count:<6} | {valid_user_action_avg:<6.1f} |"
    active_row += "\n"
    
  print(header + sub_header + header_line + columns + line + all_row + active_row + line)
  print(f"Time: {elapsed_time:.4f}s")


def main():
  start_time = time.time()
  args = parse_args()
  log_lines = download_log_file(args.url)
  sessions = parse_logs(log_lines, args.date)
  results = calculate_aggregate(sessions)

  # (13). 集計処理にかかった時間（単位は秒）
  elapsed_time = time.time() - start_time
  print_results(results, elapsed_time, args.active, args.state, args.action)

main()
