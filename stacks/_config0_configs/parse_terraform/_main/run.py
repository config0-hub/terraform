# duplicate wertqttetqwetwqtqwt
def _insert_standard_resource_labels(values):

    std_labels_keys = ["region",
                       "provider",
                       "source_method",
                       "resource_type"]

    for key in std_labels_keys:

        if not values.get(key):
            print('source standard label key "{}" not found'.format(key))
            continue

        label_key = "label-{}".format(key)

        if values.get(label_key):
            print('label key "{}" already found'.format(label_key))
            continue

        values[label_key] = values[key]

def run(stackargs):

    '''

    # resource add 
    # resource_type=credentials \
    # name=test_credentials 
    # provider=self \
    # cred_type=test_type \
    # values='{"USERNAME":"test_user","PASSWORD":"test_password"}'

    '''

    import json

    # instantiate authoring stack
    stack = newStack(stackargs)

    # query parameters
    stack.parse.add_required(key="src_resource_type")
    stack.parse.add_optional(key="src_resource_name",default="null") 
    stack.parse.add_optional(key="src_provider",default="null") 
    stack.parse.add_optional(key="src_labels",default="null") 

    stack.parse.add_required(key="dst_terraform_type")
    stack.parse.add_required(key="dst_resource_type")
    stack.parse.add_optional(key="dst_terraform_mode",default="null")
    stack.parse.add_optional(key="dst_prefix_name",default="null")

    stack.parse.add_optional(key="match",default="null") 

    stack.parse.add_optional(key="id",default="null") 
    stack.parse.add_optional(key="must_exists",default="true") 

    stack.parse.add_optional(key="add_values",default="null")
    stack.parse.add_optional(key="labels",default="null")
    stack.parse.add_optional(key="tags",default="null")

    # mapping is for additing additional fields based on another field
    # for example, id is the same as sg_id for security group id
    # this allows for querying or lookup on a more logical field
    stack.parse.add_optional(key="mapping",default="null")

    # Initialize 
    stack.init_variables()

    add_values = None

    if stack.get_attr("add_values"):
        try:
            add_values = json.loads(stack.add_values)
        except:
            add_values = None

    mapping = None

    if stack.get_attr("mapping"):
        try:
            mapping = json.loads(stack.mapping)
        except:
            mapping = None

    stack.set_parallel()

    # get terraform resource
    if stack.get_attr("match"):
        match = stack.match
    else:
        match = { "must_exists":stack.must_exists,
                  "resource_type": stack.src_resource_type }

        if stack.get_attr("src_resource_name"):
            match["name"] = stack.src_resource_name

        if stack.get_attr("id"):
            match["id"] = stack.id

        if stack.get_attr("src_provider"):
            match["provider"] = stack.src_provider

    _resource_info = list(stack.get_resource(**match))[0]
    main_id = _resource_info.get("_id")
    main_stateful_id = _resource_info.get("stateful_id")

    # changed 45234532 - moved over to b64 hash
    try:
        data = stack.b64_decode(_resource_info["raw"]["terraform"])
    except:
        data = None

    if not data:
        try:
            data = _resource_info["raw"]["terraform"]
        except:
            data = None

    if not data:
        raise Exception("expected terraform state in raw - not found.")

    for resource in data["resources"]:
        for instance in resource["instances"]:
            if stack.get_attr("dst_terraform_type") != resource.get("type"): 
                continue

            values = instance["attributes"]
            values["resource_type"] = stack.dst_resource_type
            values["query_only"] = True

            if add_values:
                for _k,_v in add_values.items():
                    values[_k] = _v

            if mapping:
                for _k,_v in mapping.items():
                    if not values.get(_k):
                        continue
                    values[_v] = values[_k]

            _results = {}

            if not values.get("name") and resource.get("name"):

                if stack.get_attr("dst_prefix_name"):
                    values["name"] = "{}-{}".format(stack.dst_prefix_name,resource["name"])
                else:
                    values["name"] = resource["name"]

            if stack.get_attr("src_provider"):
                values["provider"] = stack.src_provider
                _results["provider"] = stack.src_provider

            if stack.get_attr("cluster"): 
                values["cluster"] = stack.cluster
                _results["cluster"] = stack.cluster
                
            if stack.get_attr("instance"): 
                values["instance"] = stack.instance
                _results["instance"] = stack.instance

            if stack.get_attr("schedule_id"): 
                values["schedule_id"] = stack.schedule_id
                _results["schedule_id"] = stack.schedule_id

            if stack.get_attr("job_instance_id"): 
                values["job_instance_id"] = stack.job_instance_id
                _results["job_instance_id"] = stack.job_instance_id

            if stack.get_attr("run_id"): 
                values["run_id"] = stack.run_id
                _results["run_id"] = stack.run_id

            # AWS specific changes
            if values.get("provider") in [ "aws", "ec2" ] and values.get("arn"):

                if not values.get("region"): 
                    values["region"] = values["arn"].split(":")[3]

                if values.get("tags"): 
                    if isinstance(values["tags"],dict):
                        values["tags"] = list(values["tags"].values())
                    else:
                        del values["tags"]

            _id = stack.get_hash_object(values)
            values["_id"] = _id
            values["id"] = _id
            values["parent"] = {}

            if main_id:
                values["parent"]["_id"] = main_id
                values[main_id] = True

            if main_stateful_id:
                values["parent"]["stateful_id"] = main_stateful_id
                values[main_stateful_id] = True

            _insert_standard_resource_labels(values)

            values["source_method"] = "parse_terraform"

            _results["values"] = values
            _results["human_description"] = 'Adding resource_type "{}" id "{}"'.format(values.get("resource_type"),
                                                                                       _id)

            if stack.get_attr("labels"):
                _results["labels"] = stack.labels

            if stack.get_attr("tags"):
                _results["tags"] = stack.tags

            stack.add_resource(**_results)

    return stack.get_results()
