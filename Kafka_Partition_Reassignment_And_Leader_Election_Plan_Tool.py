#!/usr/bin/python3
"""
Kafka Partition Reassignment Plan Generator
==========================================
This script automatically generates a partition reassignment plan and leader election plan for Apache Kafka.
------------------------------------------------------------------------------
AUTHOR: Akshay Anil Utale (autale@confluent.io)
------------------------------------------------------------------------------
"""
import subprocess
import json
import re
import sys
import shlex
import logging
import time
import random
import textwrap
from datetime import datetime

LOG_FILE = "kafka_script.log"
DIVIDER = '-' * 90 # Standardized divider line

# -------- LOGGING & EXIT HANDLING --------
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
def log(msg): print(msg); logging.info(msg)
def log_error(msg): print(msg); logging.error(msg)
def log_warn(msg): print(msg); logging.warning(msg)
def log_to_file_only(msg): logging.info(msg)

def handle_exit(message="Exiting script. Please find the generated files under current working directory. Goodbye!"):
    """Prints a goodbye message and exits the script cleanly."""
    print(f"\n\n{message}")
    logging.info(message)
    sys.exit(0)

# -------- CLI UI HELPERS --------
def print_block(header_lines, divider='-', include_bottom_divider=True):
    """Prints content within a standardized block, preserving indentation on wrapped lines."""
    width = len(DIVIDER)
    print(divider * width)
    
    for line in header_lines:
        leading_spaces = len(line) - len(line.lstrip(' '))
        line_content = line.lstrip(' ')
        prefix = '  ' + ' ' * leading_spaces
        wrap_width = width - len(prefix)
        
        wrapped_lines = textwrap.wrap(
            line_content, 
            width=wrap_width, 
            break_long_words=True,
            break_on_hyphens=True
        )
        
        if not wrapped_lines:
            print()
        else:
            for wrapped_line in wrapped_lines:
                print(f"{prefix}{wrapped_line}")
        
    if include_bottom_divider:
        print(divider * width)


def display_and_log_topic_summary(topics_data):
    """Prints summary counts to CLI and logs a complete list of topics to file."""
    internal, custom, _ = classify_topics(topics_data)
    log(f"[INFO] Loaded {len(custom)} Custom Topics and {len(internal)} Internal Topics.")
    
    log_to_file_only("="*50)
    log_to_file_only("Detailed Topic Listing:")
    log_to_file_only(f"  Custom Topics ({len(custom)}):")
    for t in sorted(custom.keys()): 
        log_to_file_only(f"    - {t}")
    
    log_to_file_only(f"  Internal Topics ({len(internal)}):")
    for t in sorted(internal.keys()): 
        log_to_file_only(f"    - {t}")
    log_to_file_only("="*50)

def print_totals(topics_data):
    """Prints the total counts of loaded topics and partitions to the console."""
    total_partitions = sum(len(meta['partitions']) for meta in topics_data.values())
    msg1 = f" Total Topics Loaded: {len(topics_data)}"
    msg2 = f" Total Partitions in Scope: {total_partitions}"
    print(msg1)
    print(msg2)
    logging.info(msg1.strip())
    logging.info(msg2.strip())

def print_next_steps_section(outfile, bootstrap, command_config, msgtype='reassignment', custom_head='', election_file=None):
    """Prints the Next Steps section, relying on print_block for all formatting."""
    opcli = f"kafka-reassign-partitions --bootstrap-server {bootstrap} "
    if command_config: opcli += f"--command-config {shlex.quote(command_config)} "
    opcli += f"--reassignment-json-file {outfile} --execute"
    
    vcli = opcli.replace("--execute", "--verify")
    
    tcli = f"kafka-topics --bootstrap-server {bootstrap} "
    if command_config: tcli += f"--command-config {shlex.quote(command_config)} "
    tcli += f"--describe --topic <YOUR_TOPIC>"

    lines = [ custom_head, "", "NEXT STEPS", ""] if custom_head else ["NEXT STEPS", ""]
    
    lines.extend([
        " 1. To execute, run:", f"    {opcli}", "",
        " 2. To verify, run:", f"    {vcli}", "",
        " 3. To inspect assignments:", f"    {tcli}", ""
    ])

    if msgtype == "leader":
        election_cmd = f"kafka-leader-election --bootstrap-server {bootstrap} "
        if command_config: election_cmd += f"--command-config {shlex.quote(command_config)} "
        election_cmd += f"--path-to-json-file {election_file or '<election.json>'} --election-type preferred"
        
        lines.extend([
            " 4. After partitions sync, run preferred leader election:", f"    {election_cmd}", "",
            " 5. Inspect assignments post-election to confirm leader."
        ])

    print_block(lines, include_bottom_divider=False)

def print_welcome_banner():
    """Prints a styled welcome banner for the script's startup."""
    title = "Kafka Partition Reassignment & Leader Election Plan Tool"
    width = len(title) + 4
    print("┌" + "─" * width + "┐")
    print(f"│  {title}  │")
    print("└" + "─" * width + "┘")

# -------- CORE FUNCTIONALITY --------
def prompt_nonempty(prompt, example=None):
    """Prompts user for input, ensuring it is not empty and handling Ctrl+C."""
    example_str = f" (e.g., {example})" if example else ""
    while True:
        try:
            val = input(f"{prompt}{example_str}:\n> ").strip()
            if val:
                log_to_file_only(f"User input for '{prompt}': {val}")
                return val
            else:
                log_warn("Input cannot be empty. Please provide a valid value.")
                log_to_file_only("User provided empty input.")
        except KeyboardInterrupt:
            log_warn("\nExit request detected.")
            choice = input("Do you want to (E)xit the script or (C)ontinue with the current prompt? [C]: ").strip().lower()
            log_to_file_only(f"User responded to exit prompt with: '{choice}'")
            if choice.startswith('e'):
                handle_exit("Exiting upon user request.")
            else:
                print("Continuing...")

def prompt_confirm(prompt):
    """Prompts user for a yes/no confirmation, requiring an explicit 'y' or 'n' answer."""
    while True:
        try:
            val = input(f"{prompt} [y/n]:\n> ").strip().lower()
            log_to_file_only(f"User confirmation for '{prompt}': {val}")
            
            if val.startswith('y'):
                return True
            elif val.startswith('n'):
                return False
            else:
                log_warn("Invalid input. Please enter 'y' for Yes or 'n' for No.")
        except KeyboardInterrupt:
            log_warn("\nExit request detected.")
            choice = input("Do you want to (E)xit the script or (C)ontinue with the current prompt? [C]: ").strip().lower()
            log_to_file_only(f"User responded to exit prompt with: '{choice}'")
            if choice.startswith('e'):
                handle_exit("Exiting upon user request.")
            else:
                print("Continuing...")

def prompt_for_bootstrap_server():
    """Prompts for the Kafka bootstrap server."""
    return prompt_nonempty("Enter the Kafka bootstrap server (host:port)", "kafka.example.com:9092")

def prompt_for_topic_filter():
    """Prompts the user for the topic name filter."""
    return prompt_nonempty("\nEnter a topic name filter for initial load (substring, regex, or * for all)")

def fetch_and_parse_topics(bootstrap, command_config, filter_input):
    """Fetches topic metadata from Kafka, filters it, and parses the output."""
    try:
        name_filter = re.compile(filter_input) if filter_input and filter_input != '*' else None
    except re.error as e:
        log_error(f"Invalid regex '{filter_input}': {e}")
        return None

    cmd = ["kafka-topics", "--describe", "--bootstrap-server", bootstrap]
    if command_config: cmd.extend(["--command-config", command_config])
    
    print(DIVIDER)
    log("[INFO] Fetching topic metadata from Kafka cluster...")
    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True, timeout=120)
        log_to_file_only("="*50 + "\nRAW KAFKA-TOPICS OUTPUT\n" + "="*50 + f"\nCOMMAND: {' '.join(cmd)}\n\n{proc.stdout}\n" + "="*50)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, FileNotFoundError) as e:
        log_error(f"Failed to execute kafka-topics: {getattr(e, 'stderr', e)}")
        return None

    topics = {}
    parser_topic = re.compile(r'^\s*Topic:\s*([^\s]+).*ReplicationFactor:\s*(\d+)')
    parser_replica = re.compile(r'^\s*Topic:\s*([^\s]+).*Partition:\s*(\d+).*Replicas:\s*([0-9,]+)')

    for line in proc.stdout.splitlines():
        if (m := parser_topic.match(line)):
            topic, rf = m.groups()
            if not name_filter or name_filter.search(topic):
                if topic not in topics: topics[topic] = {'rf': int(rf), 'partitions': []}
        elif (m := parser_replica.match(line)):
            topic, part, repl = m.groups()
            if topic in topics:
                topics[topic]['partitions'].append({'num': int(part), 'replicas': [int(r) for r in repl.split(',')]})
    
    if not topics: 
        log_warn("No topics found matching the filter.")
        return {}
    
    return topics

def classify_topics(topics):
    internal = {t: m for t, m in topics.items() if t.startswith('_')}
    custom = {t: m for t, m in topics.items() if not t.startswith('_')}
    total_partitions = sum(len(m['partitions']) for m in topics.values())
    return internal, custom, total_partitions

def get_brokers_from_topic_metadata(topics):
    """Compiles a list of broker IDs from the replicas of loaded topics."""
    broker_ids = set()
    for meta in topics.values():
        for p in meta['partitions']: broker_ids.update(p['replicas'])
    return sorted(list(broker_ids))

# -------- REASSIGNMENT PLAN GENERATORS --------
def format_reassignment_json(data):
    """Creates a beautifully formatted JSON string with single-line replica arrays."""
    lines = ['{', '  "version": 1,', '  "partitions": [']
    partition_lines = []
    for p in data['partitions']:
        replicas_str = ', '.join(map(str, p['replicas']))
        partition_lines.append(
            '    {\n'
            f'      "topic": "{p["topic"]}",\n'
            f'      "partition": {p["partition"]},\n'
            f'      "replicas": [{replicas_str}]\n'
            '    }'
        )
    lines.append(',\n'.join(partition_lines))
    lines.extend(['  ]', '}'])
    return '\n'.join(lines)

def generate_reassignment_plan_file(plan_partitions, outfile):
    """Writes the final plan to a file using the custom formatter."""
    reassignment_object = {"version": 1, "partitions": plan_partitions}
    with open(outfile, "w") as f:
        f.write(format_reassignment_json(reassignment_object))

def run_rebalance_partitions(topics, bootstrap, command_config):
    """Action for rebalancing partitions. Returns a status message on cancellation."""
    print_block(["Action: Build a reassign partitions plan"], include_bottom_divider=False)
    all_broker_ids = get_brokers_from_topic_metadata(topics)
    if not all_broker_ids:
        log_error("Could not compile a broker list from loaded topics. Aborting action."); return ""

    internal, custom, total_partitions = classify_topics(topics)
    print_block([
        "Plan Generation Summary",
        f"▪ Operating on {len(custom)} Custom and {len(internal)} Internal topics.",
        f"▪ Generating a new replica distribution for {total_partitions} partitions.",
        f"▪ Brokers available for assignment: {all_broker_ids}"
    ])
    
    if not prompt_confirm("Proceed with generating the reassignment plan?"):
        return "Operation cancelled."

    plan_partitions = []
    for topic, meta in topics.items():
        for p in meta['partitions']:
            rf = len(p['replicas'])
            if rf > len(all_broker_ids):
                log_warn(f"Skipping {topic}-{p['num']}: RF ({rf}) > available brokers ({len(all_broker_ids)})."); continue
            
            new_replicas = random.sample(all_broker_ids, rf)
            if len(all_broker_ids) > rf:
                while set(new_replicas) == set(p['replicas']):
                    new_replicas = random.sample(all_broker_ids, rf)
            
            plan_partitions.append({'topic': topic, 'partition': p['num'], 'replicas': new_replicas})
    
    outfile = "reassign-partitions-plan.json"
    generate_reassignment_plan_file(plan_partitions, outfile)
    
    print(DIVIDER)
    success_msg = f"[SUCCESS] Wrote rebalance plan to '{outfile}' for {len(plan_partitions)} partition(s)."
    for line in textwrap.wrap(success_msg, width=len(DIVIDER)):
        log(line)
        
    print_next_steps_section(outfile, bootstrap, command_config)
    return "" # Return empty string on success

def run_leader_election(topics, bootstrap, command_config):
    """Action for leader election. Returns a status message on cancellation."""
    print_block(["Action: Build a preferred leader election plan."])
    topic_list = sorted(topics.keys())
    
    while True:
        log_to_file_only("Topics currently loaded:")
        for t in topic_list:
            log_to_file_only(f"  - {t}")
        
        log(f"There are {len(topic_list)} topics currently in scope. See the log file for a complete list: {LOG_FILE}")
        choice = prompt_nonempty("Apply to (A)ll topics, or specify a (F)ilter? [A/F]")
        
        selected_topics = set()
        if choice.lower().startswith('a'):
            selected_topics = set(topic_list)
        elif choice.lower().startswith('f'):
            entry = prompt_nonempty("\nEnter a topic name filter (substring, comma-list, or * for all)")
            tokens = [tok.strip() for tok in entry.split(',')]
            selected_topics = {t for t in topic_list if entry == '*' or any(tok in t for tok in tokens)}
        else:
            log_warn("Invalid choice. Please enter 'A' for All or 'F' for Filter.")
            continue
        # --- END OF MODIFIED SECTION ---

        if not selected_topics: log_error("No loaded topics matched your selection. Please try again."); continue
        
        selected_partitions = set()
        print()
        if not prompt_confirm(f"Proceed with {len(selected_topics)} selected topics?"):
            return "Operation cancelled."

        for t in selected_topics: selected_partitions.update((t, p['num']) for p in topics[t]['partitions'])

        if not selected_partitions: log_error("No partitions were selected. Please try again."); continue
            
        all_broker_ids = get_brokers_from_topic_metadata(topics)
        print(DIVIDER)
        log(f"Brokers available based on loaded topics: {all_broker_ids}")
        print(DIVIDER)
        
        while True:
            leader_choice = prompt_nonempty("Enter the Broker ID to be the new leader")

            try:
                leader_broker = int(leader_choice)
            except ValueError:
                log_error("Invalid Broker ID. Please enter a number.")
                print(DIVIDER)
                continue

            if leader_broker not in all_broker_ids:
                log_error(f"Error: Broker ID {leader_broker} is not in the list of available brokers: {all_broker_ids}")
                print(DIVIDER)
                continue
            
            break
        
        print_block([ "CONFIRMATION", "", f"Action: Set Broker {leader_broker} as the preferred leader.", f"Scope: {len(selected_partitions)} partitions across {len(selected_topics)} topics.", ""])

        if prompt_confirm("Proceed with generating the plans?"):
            reassign_plan = []
            for topic, p_num in selected_partitions:
                current_replicas = next(part['replicas'] for part in topics[topic]['partitions'] if part['num'] == p_num)
                new_replicas = [leader_broker] + [r for r in current_replicas if r != leader_broker]
                reassign_plan.append({'topic': topic, 'partition': p_num, 'replicas': new_replicas})
            
            outfile_reassign, outfile_election = "reassign-partitions-for-leader-plan.json", "leader-election-plan.json"
            generate_reassignment_plan_file(reassign_plan, outfile_reassign)
            with open(outfile_election, "w") as f: json.dump({'partitions': [{'topic': t, 'partition': p} for t, p in selected_partitions]}, f, indent=2)

            summary = f"[INFO] Writing Kafka Partition Reassignment plan & Leader Election Plan For ({len(selected_partitions)} partition(s), setting leader: broker {leader_broker})"
            success_msg = f"[SUCCESS] Wrote reassignment plan to '{outfile_reassign}' and leader election plan to '{outfile_election}'."
            
            print_block([summary, "", success_msg], include_bottom_divider=False)

            print_next_steps_section(outfile_reassign, bootstrap, command_config, msgtype="leader", custom_head="", election_file=outfile_election)
            return ""
        
        elif prompt_confirm("Would you like to restart the selection process?"): 
            continue
        else: 
            return "Leader election action cancelled."

# --------- MAIN WORKFLOW ---------
def main():
    log_to_file_only(f"Kafka Administrative Utility started at {datetime.now().isoformat()}")
    
    print_welcome_banner()

    print(f"{DIVIDER}")
    print("Connection Setup and Initial Data Load")
    print(DIVIDER)
    bootstrap = prompt_for_bootstrap_server()
    command_config = prompt_nonempty("\nEnter full path to client properties file", "/etc/kafka/client.properties") if prompt_confirm("\nIs security enabled (SASL/SSL)?") else None
    
    filter_input = prompt_for_topic_filter()
    
    print(DIVIDER)
    connected_msg = f"Connected to: {bootstrap}"
    for line in textwrap.wrap(connected_msg, width=len(DIVIDER)):
        log(line)
    
    topics = fetch_and_parse_topics(bootstrap, command_config, filter_input)
    
    if topics is None: 
        handle_exit("Could not load initial topic data. Please check configuration and permissions.")
    
    display_and_log_topic_summary(topics)
    print_totals(topics)
    print(DIVIDER)

    log("Loading complete. Proceeding to main menu...")
    
    while True:
        print(f"{DIVIDER}")
        print("Select an operation:")
        print("  1. Build a reassign partitions plan.")
        print("  2. Build a preferred leader election plan.")
        print("  3. Exit")
        which = prompt_nonempty("\nChoose [1/2/3]")

        status_message = ""
        if which == '1': status_message = run_rebalance_partitions(topics, bootstrap, command_config)
        elif which == '2': status_message = run_leader_election(topics, bootstrap, command_config)
        elif which == '3': break 
        else: log_warn("Invalid option. Please try again."); continue
        
        print(DIVIDER)
        prompt_str = "Perform another operation? [Y]es / [R]efresh data & continue / [N]o:\n> "
        
        if status_message:
            prompt_str = f"{status_message}. {prompt_str}"

        continue_choice = input(prompt_str).strip().lower()
        
        if continue_choice.startswith('r'):
            print()
            log("Refreshing Topic Data...")
            print(DIVIDER)
            
            refresh_filter = prompt_for_topic_filter()
            
            new_topics = fetch_and_parse_topics(bootstrap, command_config, filter_input)
            
            
            if new_topics is not None: 
                topics = new_topics
                log("Topic data refreshed successfully.")
                print_totals(topics)
            else: 
                log_error("Failed to refresh topic data. Keeping existing data set.")
            
        elif not continue_choice.startswith('y'): break

if __name__ == "__main__":
    try:
        main()
        handle_exit("Script finished. Goodbye!")
    except KeyboardInterrupt:
        handle_exit("\nOperation cancelled by user (global handler). Goodbye!")
    except Exception as e:
        log_error(f"\nAn unexpected error occurred: {e}")
        logging.exception("Caught unhandled exception:")
        handle_exit("Script exited due to an unexpected error.")
