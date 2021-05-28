from fetching import *


def flatten(info):
    yield info
    for step in info["steps"]:
        if "sfnExecutionInfo" in step:
            yield from flatten(step["sfnExecutionInfo"])
            step["sfnExecutionArn"] = step["sfnExecutionInfo"]["sfnExecutionArn"]
            step["notes"] = render_html_element(
                "a",
                {"href": f'#{step["sfnExecutionArn"]}'},
                children=[step["sfnExecutionInfo"]["sfnExecutionArn"]],
            )
            del step["sfnExecutionInfo"]
        if "emrClusterInfo" in step:
            yield step["emrClusterInfo"]
            step["emrClusterId"] = step["emrClusterInfo"]["emrClusterId"]
            step["emrClusterName"] = step["emrClusterInfo"]["emrClusterName"]
            step["notes"] = render_html_element(
                "a",
                {"href": f'#{step["emrClusterId"]}'},
                children=[step["emrClusterInfo"]["emrClusterArn"]],
            )
            del step["emrClusterInfo"]


def render_html_page(info):
    html_elements = []
    logs = []

    flat_list = list(flatten(info))
    print(f"FLATTENED: {json.dumps(flat_list)}")

    for item in flat_list:
        if "sfnExecutionArn" in item:
            html_elements += render_html_for_sfn(item)
        if "emrClusterId" in item:
            for step in item["steps"]:
                if "dataVolume" in step:
                    step["dataVolume"] = str(step["dataVolume"])
                if "error" in step:
                    step["notes"] = f'ERROR: {step["error"]}'
                if "logPath" in step:
                    log_id = f'{item["emrClusterId"]}_{step["stepId"]}'
                    log_file = f'{step["logPath"]}stderr.gz'
                    logs.append(
                        {
                            "emrClusterArn": item["emrClusterArn"],
                            "stepName": step["stepName"],
                            "logId": log_id,
                            "content": download_logs(log_file),
                        }
                    )
                    step["notes"] += " | "
                    step["notes"] += render_html_element(
                        "a", {"href": f"#{log_id}"}, children=["Log"]
                    )
            html_elements += render_html_for_emr(item)

    if len(logs) > 0:
        html_elements += render_html_element("h2", children=["Logs"])

    for log in logs:
        html_elements += render_html_element(
            "h3",
            {"id": log["logId"]},
            children=[f'EMR: {log["emrClusterArn"]} --- STEP: {log["stepName"]}'],
        )
        html_elements += render_html_element(
            "textarea", {"readonly": "true"}, children=[log["content"]]
        )

    with open("summary.css", "r") as css_file:
        css = css_file.read()

    return render_html_element(
        "html",
        children=[
            render_html_element("head", children=[render_html_element("style", children=[css])]),
            render_html_element("body", children=html_elements),
        ],
    )


def render_html_for_sfn(item):
    exec_arn = item["sfnExecutionArn"]
    region = extract_region_from_arn(exec_arn)
    console_link = f"https://{region}.console.aws.amazon.com/states/home?region={region}#/executions/details/{exec_arn}"

    return [
        render_html_element("h2", {"id": exec_arn}, children=[f"SFN: {exec_arn}"]),
        render_html_element(
            "p",
            children=[
                "Status: ",
                render_html_element(
                    "span",
                    attributes={"class": "success" if item["status"] == "SUCCEEDED" else "failure"},
                    children=[item["status"]],
                ),
                " | ",
                render_html_element(
                    "a",
                    {"href": console_link, "target": "_blank"},
                    children=["Open in AWS Console"],
                ),
            ],
        ),
        render_html_element("h3", children=["Input"]),
        render_html_element("pre", children=[json.dumps(item["input"], indent=2)]),
        render_html_table(
            {
                "stepName": "Step Name",
                "duration": "Duration",
                "status": "Status",
                "notes": "Notes",
            },
            item["steps"],
        ),
    ]


def render_html_for_emr(item):
    return [
        render_html_element(
            "h2", {"id": item["emrClusterId"]}, children=[f'EMR: {item["emrClusterArn"]}']
        ),
        render_html_element(
            "p",
            children=[
                "Status: ",
                render_html_element("b", children=[item["status"]]),
                " | ",
                render_html_element(
                    "a",
                    {"href": item["emrClusterLink"], "target": "_blank"},
                    children=["Open in AWS Console"],
                ),
            ],
        ),
        render_html_table(
            {
                "stepName": "Step Name",
                "duration": "Duration",
                "dataVolume": "Data Volume (MB)",
                "status": "Status",
                "notes": "Notes",
            },
            item["steps"],
        ),
    ]


def render_html_table(headers, rows=[]):
    rows_markup = []

    rows_markup.append(
        render_html_element(
            "tr",
            children=[render_html_element("th", children=[headers[k]]) for k in headers.keys()],
        )
    )

    for row in rows:
        rows_markup.append(
            render_html_element(
                "tr",
                children=[
                    render_html_element("td", children=[row[k] if k in row.keys() else ""])
                    for k in headers.keys()
                ],
            )
        )

    return render_html_element("table", children=rows_markup)


def render_html_element(name, attributes={}, children=[]):
    attributes_markup = ""
    for key, value in attributes.items():
        attributes_markup += f' {key}="{value}"'

    content = "".join(children)

    return f"<{name}{attributes_markup}>{content}</{name}>"
