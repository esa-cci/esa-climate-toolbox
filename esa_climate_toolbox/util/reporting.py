import inspect
import os
import sys
import traceback
import requests
import esa_climate_toolbox

class FailureReporter:

    def __init__(self, server_url = None):
        if server_url is not None:
            self.server_url = server_url
        else:
            self.server_url = os.environ.get(
                "ESA_CLIMATE_TOOLBOX_FAILURE_REPORTING_SERVER_URL",
                "http://localhost:9898"
            )

    def _report_exception(self, e_class, e_instance, tb):
        current_tb = tb
        while current_tb is not None:
            if "/esa_climate_toolbox/" in current_tb.tb_frame.f_code.co_filename:
                # ECT is somewhere in the call stack, so we will report.
                break
            current_tb = current_tb.tb_next
        else:
            # ECT is not involved, so ignore this exception.
            return
        print("Reporting exception.")
        data = {
            "toolbox-version": "unset",
            "exception-class": e_class.__name__,
            "exception-instance": repr(e_instance),
            "traceback": traceback.format_tb(tb)
        }
        try:
            data["toolbox-version"] = esa_climate_toolbox.__dict__.get(
                "__version__",
                "unknown"
            )
        except ModuleNotFoundError:
            # Nothing to do here -- we just omit the version
            # information.
            pass
        requests.post(
            self.server_url,
            json=data
        )

    def _patch_excepthook(self):
        old_hook = sys.excepthook

        def handler(e_class, e_instance, tb):
            self._report_exception(e_class, e_instance, tb)
            old_hook(e_class, e_instance, tb)

        sys.excepthook = handler

    def _patch_ipython(self):
        def handler(handler_self, e_class, e_instance, tb, tb_offset=None):
            # This function will be turned into an instance method
            # of InteractiveShell, hence the "handler_self".
            self._report_exception(e_class, e_instance, tb)
            handler_self.showtraceback()

        # This method is only called when we know that get_ipython()
        # can be resolved.
        # noinspection PyUnresolvedReferences
        get_ipython().set_custom_exc((Exception,), handler)

    def activate(self):
        print("Please enter YES below to consent to reporting.")
        if input().lower() != "yes":
            print("Reporting not activated.")
            return
        print("Activating reporting.")
        parent_frame = inspect.currentframe().f_back
        if "get_ipython" in (globals() | locals() | parent_frame.f_globals | parent_frame.f_locals):
            # We're in an IPython environment, so normal excepthook patching
            # won't work.
            self._patch_ipython()
            print("IPython reporting activated.")
        else:
            self._patch_excepthook()
            print("Non-IPython reporting activated.")

    @staticmethod
    def raise_error():
        raise Exception("This is an error to test the reporting functionality.")
