import sys
import json

def main():
    old_container = sys.argv[1]
    new_containers_json = sys.argv[2]
    jobdef_file = sys.argv[3]
    import ROOT
    input_file = ROOT.TFile.Open(jobdef_file,"UPDATE")
    metadata_object = input_file.Get(old_container)
    with open(new_containers_json, "r") as f:
        containers = json.loads(f.read())
    for container in containers:
        if container == old_container:
            continue
        input_file.WriteTObject(metadata_object,container)
    input_file.Close()

    pass

if __name__ == "__main__":
    main()