def _report_exception(e_class, e_instance, tb):
    import traceback
    import requests
    print("Reporting exception.")
    data = {
        "toolbox-version": "unset",
        "exception-class": e_class.__name__,
        "exception-instance": repr(e_instance),
        "traceback": traceback.format_tb(tb)
    }
    try:
        import esa_climate_toolbox as ect
        data["toolbox-version"] = ect.__dict__.get(
            "__version__",
            "unknown"
        )
    except ModuleNotFoundError:
        # Nothing to do here -- we just omit the version
        # information.
        pass
    requests.post(
        "http://localhost:9898",
        json=data
    )


def _patch_excepthook():
    import sys
    old_hook = sys.excepthook

    def handler(e_class, e_instance, tb):
        _report_exception(e_class, e_instance, tb)
        old_hook(e_class, e_instance, tb)

    sys.excepthook = handler


def _patch_ipython():
    ishell: InteractiveShell = get_ipython()

    def handler(self, e_class, e_instance, tb, tb_offset=None):
        _report_exception(e_class, e_instance, tb)
        ishell.showtraceback()

    ishell.set_custom_exc((Exception,), handler)


def activate_reporting():
    print("Please enter YES below to consent to reporting.")
    if input().lower() != "yes":
        print("Reporting not activated.")
        return
    print("Activating reporting.")
    if "get_ipython" in (globals() | locals()):
        # We're in an IPython environment, so normal excepthook
        # patching won't work.
        _patch_ipython()
    else:
        _patch_excepthook()
