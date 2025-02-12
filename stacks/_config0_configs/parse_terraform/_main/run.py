import boto3
import base64
import json

def s3_to_tfstate(bucket_name, stateful_id):

    bucket_key = f'{stateful_id}/state/{stateful_id}.tfstate'

    s3 = boto3.client('s3')

    response = s3.get_object(Bucket=bucket_name,
                             Key=bucket_key)

    base64_data = response['Body'].read().decode('utf-8')

    try:
        json_data = base64.b64decode(base64_data).decode('utf-8')
    except:
        json_data = base64_data

    # Deserialize the JSON string back to a dictionary
    data = json.loads(json_data)

    return data

# duplicate wertqttetqwetwqtqwt
def _insert_standard_resource_labels(values):

    std_labels_keys = ["region",
                       "provider"
                       ]

    for key in std_labels_keys:

        if not values.get(key):
            print('source standard label key "{}" not found'.format(key))
            continue

        label_key = "label-{}".format(key)

        if values.get(label_key):
            print('label key "{}" already found'.format(label_key))
            continue

        values[label_key] = values[key]

def _get_src_resource(stack):

    match = { "must_exists":stack.must_exists,
              "resource_type": stack.src_resource_type }

    if stack.get_attr("src_resource_name"):
        match["name"] = stack.src_resource_name

    if stack.get_attr("src_provider"):
        match["provider"] = stack.src_provider

    return list(stack.get_resource(**match))[0]

def _get_parent_info(resource_info):

    parent_info = {
    "_id":resource_info["_id"] }

    if resource_info.get("stateful_id"):
        parent_info["stateful_id"] = resource_info["stateful_id"]

    if resource_info.get("remote_stateful_bucket"):
        parent_info["remote_stateful_bucket"] = resource_info["remote_stateful_bucket"]

    return parent_info

def run(stackargs):

    import json

    # instantiate authoring stack
    stack = newStack(stackargs)

    # query parameters
    stack.parse.add_required(key="src_resource_type")
    stack.parse.add_optional(key="src_resource_name",default="null") 
    stack.parse.add_optional(key="src_provider",default="null") 
    stack.parse.add_optional(key="src_labels",default="null")
    stack.parse.add_optional(key="must_exists",default="true")

    stack.parse.add_required(key="dst_terraform_type")
    stack.parse.add_required(key="dst_resource_type")
    stack.parse.add_optional(key="dst_prefix_name",default="null")
    stack.parse.add_optional(key="add_values",default="null")

    # mapping is for adding fields
    stack.parse.add_optional(key="mapping",default="null")

    # Initialize 
    stack.init_variables()

    add_values = None
    mapping = None

    if stack.get_attr("add_values"):
        try:
            add_values = json.loads(stack.add_values)
        except:
            add_values = None

    if stack.get_attr("mapping"):
        try:
            mapping = json.loads(stack.mapping)
        except:
            mapping = None

    # get terraform resource
    src_resource = _get_src_resource(stack)

    data = s3_to_tfstate(src_resource["remote_stateful_bucket"],
                         src_resource["stateful_id"])

    stack.set_parallel()

    transfer_keys = [
        "cluster",
        "instance",
        "schedule_id",
        "job_instance_id",
        "run_id"
    ]

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

            for tkey in transfer_keys:

                if not stack.get_attr(tkey):
                    continue

                values[tkey] = getattr(stack, tkey)
                _results[tkey] = getattr(stack, tkey)

            # AWS specific changes
            if values.get("provider") in [ "aws", "ec2" ] and values.get("arn"):

                if not values.get("region"): 
                    values["region"] = values["arn"].split(":")[3]

                if values.get("tags") and isinstance(values["tags"],dict):
                    values["tags"] = list(values["tags"].values())
                else:
                    del values["tags"]

            _id = stack.get_hash_object(values)
            values["_id"] = _id
            values["id"] = _id
            values["parent"] = _get_parent_info(src_resource)
            _insert_standard_resource_labels(values)

            values["source_method"] = "parse_terraform"

            _results["values"] = values
            human_description = f'Adding resource_type "{values.get("resource_type")}" id "{_id}"'
            _results["human_description"] = human_description

            stack.add_resource(**_results)

    return stack.get_results()
