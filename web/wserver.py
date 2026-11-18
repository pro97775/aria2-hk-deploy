from uvloop import install

install()
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from logging import getLogger, FileHandler, StreamHandler, INFO, basicConfig, WARNING
from aioaria2 import Aria2HttpClient
from aiohttp.client_exceptions import ClientError

from web.nodes import extract_file_ids, make_tree
from web.pin import make_pin

getLogger("httpx").setLevel(WARNING)
getLogger("aiohttp").setLevel(WARNING)

aria2 = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global aria2
    aria2 = Aria2HttpClient("http://localhost:6800/jsonrpc")
    yield
    await aria2.close()


app = FastAPI(lifespan=lifespan)


templates = Jinja2Templates(directory="web/templates/")

basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[FileHandler("log.txt"), StreamHandler()],
    level=INFO,
)

LOGGER = getLogger(__name__)


@app.get("/app/files", response_class=HTMLResponse)
async def files(request: Request):
    return templates.TemplateResponse(request, "page.html")


@app.api_route(
    "/app/files/torrent", methods=["GET", "POST"], response_class=HTMLResponse
)
async def handle_torrent(request: Request):
    params = request.query_params

    if not (gid := params.get("gid")):
        return JSONResponse(
            {
                "files": [],
                "engine": "",
                "error": "GID is missing",
                "message": "GID not specified",
            }
        )

    if not (pin := params.get("pin")):
        return JSONResponse(
            {
                "files": [],
                "engine": "",
                "error": "Pin is missing",
                "message": "PIN not specified",
            }
        )

    code = make_pin(gid)
    if code != pin:
        return JSONResponse(
            {
                "files": [],
                "engine": "",
                "error": "Invalid pin",
                "message": "The PIN you entered is incorrect",
            }
        )

    if request.method == "POST":
        if not (mode := params.get("mode")):
            return JSONResponse(
                {
                    "files": [],
                    "engine": "",
                    "error": "Mode is not specified",
                    "message": "Mode is not specified",
                }
            )
        data = await request.json()
        if mode == "rename":
            return JSONResponse(
                {
                    "files": [],
                    "engine": "",
                    "error": "Rename failed.",
                    "message": "Renaming is not supported.",
                }
            )
        selected_files, _ = extract_file_ids(data)
        if not selected_files:
            return JSONResponse(
                {
                    "files": [],
                    "engine": "",
                    "error": "No files selected",
                    "message": "Select at least one file.",
                }
            )
        try:
            await aria2.changeOption(gid, {"select-file": ",".join(selected_files)})
        except (ClientError, Exception) as e:
            LOGGER.error(f"{e} Errored in selection")
            return JSONResponse(
                {
                    "files": [],
                    "engine": "",
                    "error": "Error applying selection",
                    "message": str(e),
                }
            )
        content = {
            "files": [],
            "engine": "",
            "error": "",
            "message": "Your selection has been submitted successfully.",
        }
    else:
        try:
            try:
                options = await aria2.getOption(gid)
                root_path = options.get("dir", "")
            except Exception:
                root_path = ""
            res = await aria2.getFiles(gid)
            content = make_tree(res, "aria2", root_path)
        except (ClientError, Exception) as e:
            LOGGER.error(str(e))
            content = {
                "files": [],
                "engine": "",
                "error": "Error getting files",
                "message": str(e),
            }
    return JSONResponse(content)


@app.get("/", response_class=HTMLResponse)
async def homepage():
    return (
        "<!DOCTYPE html><html><head><title>Worker Node Status</title></head>"
        "<body style='font-family:sans-serif;background:#0f172a;color:#e2e8f0;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;'>"
        "<div style='padding:24px 32px;background:#1e293b;border-radius:12px;border:1px solid #334155;'>"
        "<h2 style='margin:0 0 8px 0;color:#38bdf8;'>Cloud Sync Worker</h2>"
        "<p style='margin:0;color:#94a3b8;'>Status: Operational</p>"
        "</div></body></html>"
    )


@app.exception_handler(Exception)
async def page_not_found(_, exc):
    return HTMLResponse(
        f"<h1>404: Task not found! Mostly wrong input. <br><br>Error: {exc}</h1>",
        status_code=404,
    )
