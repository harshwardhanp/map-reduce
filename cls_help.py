class cls_help:
    help_dict = {
            "cls-help": "Getting help",
            "cls-create": "creating cluster",
            "cls-init": "initialising cluster",
            "cls-set-mapred": "setting up cluster",
            "cls-run-mapred": "running cluster",
            "cls-destroy": "destroying cluster destroys a single cluster",
            "cls-status": "getting status of a cluster",
            "cls-exit": "exiting cluster -- destroys whole cluster pool"
        }
    
    def getHelp(*command):
        if len(command) ==0:
            command = cls_help.help_dict.keys()
        for cmd in command:
            print(cls_help.help_dict[cmd])