import json
import os

j = json.load(open("table.json", "r"))
fp = open("table.go", "w")
fp.write("package commands\n\nvar containers = map[string]bool{\n")
for container in j["container"]:
    fp.write(f'"{container.upper()}": true,\n')
fp.write("}\nvar redisCommands = map[string]redisCommand{\n")

for group, cmds in j["table"].items():
    group = group.upper()
    for cmd_name, specs in cmds.items():
        print(group, cmd_name)
        cmd_name = cmd_name.upper()
        fp.write(f'"{cmd_name}": ' + "{\n")
        fp.write(f'"{group}",\n')
        fp.write("[]keySpec{\n")
        for key_spec in specs:
            fp.write("{\n")
            if "index" in key_spec["begin_search"]:
                fp.write('"index",\n')
                fp.write(f'{key_spec["begin_search"]["index"]["pos"]},\n')
                fp.write('"",\n')
                fp.write('0,\n')
            elif "keyword" in key_spec["begin_search"]:
                fp.write('"keyword",\n')
                fp.write('0,\n')
                fp.write(f'"{key_spec["begin_search"]["keyword"]["keyword"]}",\n')
                fp.write(f'{key_spec["begin_search"]["keyword"]["startfrom"]},\n')
            else:
                raise Exception(key_spec)
            if "range" in key_spec["find_keys"]:
                fp.write('"range",\n')  # type
                fp.write(f'{key_spec["find_keys"]["range"]["lastkey"]},\n')  #
                fp.write(f'{key_spec["find_keys"]["range"]["step"]},\n')  #
                fp.write(f'{key_spec["find_keys"]["range"]["limit"]},\n')  #
                fp.write('0,\n')
                fp.write('0,\n')
                fp.write('0,\n')
            elif "keynum" in key_spec["find_keys"]:
                fp.write('"keynum",\n')  # type
                fp.write('0,\n')
                fp.write('0,\n')
                fp.write('0,\n')
                fp.write(f'{key_spec["find_keys"]["keynum"]["keynumidx"]},\n')  #
                fp.write(f'{key_spec["find_keys"]["keynum"]["firstkey"]},\n')  #
                fp.write(f'{key_spec["find_keys"]["keynum"]["step"]},\n')  #
            else:
                raise Exception(key_spec)
            fp.write('},\n')
        fp.write('},\n')
        fp.write('},\n')
fp.write('}\n')
fp.close()
os.system("go fmt table.go")
