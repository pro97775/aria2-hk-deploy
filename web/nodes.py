import os

from anytree import NodeMixin


class TorNode(NodeMixin):
    def __init__(
        self,
        name,
        is_folder=False,
        is_file=False,
        parent=None,
        size=None,
        priority=None,
        file_id=None,
        progress=None,
    ):
        super().__init__()
        self.name = name
        self.is_folder = is_folder
        self.is_file = is_file

        if parent is not None:
            self.parent = parent
        if size is not None:
            self.fsize = size
        if priority is not None:
            self.priority = priority
        if file_id is not None:
            self.file_id = file_id
        if progress is not None:
            self.progress = progress


def get_folders(path, root_path):
    if root_path:
        try:
            path = os.path.relpath(path, root_path)
        except ValueError:
            pass
    return [part for part in path.split("/") if part and part not in (".", "..")]


def make_tree(res, tool, root_path=""):
    parent = TorNode("ARIA2")
    folder_id = 0
    for i in res:
        folders = get_folders(i["path"], root_path)
        priority = 1
        if i["selected"] == "false":
            priority = 0
        if len(folders) > 1:
            previous_node = parent
            for j in range(len(folders) - 1):
                current_node = next(
                    (k for k in previous_node.children if k.name == folders[j]),
                    None,
                )
                if current_node is None:
                    previous_node = TorNode(
                        folders[j],
                        is_folder=True,
                        parent=previous_node,
                        file_id=folder_id,
                    )
                    folder_id += 1
                else:
                    previous_node = current_node
            try:
                progress = round(
                    (int(i["completedLength"]) / int(i["length"])) * 100, 5
                )
            except:
                progress = 0
            TorNode(
                folders[-1],
                is_file=True,
                parent=previous_node,
                size=int(i["length"]),
                priority=priority,
                file_id=i["index"],
                progress=progress,
            )
        else:
            try:
                progress = round(
                    (int(i["completedLength"]) / int(i["length"])) * 100, 5
                )
            except:
                progress = 0
            TorNode(
                folders[-1],
                is_file=True,
                parent=parent,
                size=int(i["length"]),
                priority=priority,
                file_id=i["index"],
                progress=progress,
            )

    return {"files": create_list(parent), "engine": "aria2"}


def create_list(parent, contents=None):
    if contents is None:
        contents = []
    for i in parent.children:
        if i.is_folder:
            children = []
            create_list(i, children)
            contents.append(
                {
                    "id": f"folderNode_{i.file_id}",
                    "name": i.name,
                    "type": "folder",
                    "children": children,
                }
            )
        else:
            contents.append(
                {
                    "id": i.file_id,
                    "name": i.name,
                    "size": i.fsize,
                    "type": "file",
                    "selected": bool(i.priority),
                    "progress": i.progress,
                }
            )
    return contents


def extract_file_ids(data):
    selected_files = []
    unselected_files = []
    for item in data:
        if item.get("type") == "file":
            if item.get("selected"):
                selected_files.append(str(item["id"]))
            else:
                unselected_files.append(str(item["id"]))
        if item.get("children"):
            child_selected, child_unselected = extract_file_ids(item["children"])
            selected_files.extend(child_selected)
            unselected_files.extend(child_unselected)
    return selected_files, unselected_files
