"""Chromium launch options for Playwright (works on any machine with `playwright install chromium`)."""
import os


def chromium_launch_kwargs(executable_path=None):
    """
    Use a custom Chromium only if the path exists (argument or PLAYWRIGHT_CHROMIUM_EXECUTABLE).
    Otherwise Playwright uses its downloaded browser — no hardcoded user cache path.
    """
    exe = None
    if executable_path and os.path.isfile(executable_path):
        exe = executable_path
    else:
        env = os.environ.get("PLAYWRIGHT_CHROMIUM_EXECUTABLE", "").strip()
        if env and os.path.isfile(env):
            exe = env
    kw = {
        "headless": True,
        "args": ["--disable-http2", "--disable-blink-features=AutomationControlled"],
    }
    if exe:
        kw["executable_path"] = exe
    return kw
