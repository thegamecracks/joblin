import logging
from tkinter import Text, Tk
from tkinter.ttk import Button, Frame

from joblin import Scheduler


def enable_windows_dpi_awareness() -> None:
    import sys

    if sys.platform == "win32":
        from ctypes import windll

        windll.shcore.SetProcessDpiAwareness(2)


def main() -> None:
    fmt = "%(levelname)s: %(message)-50s (%(name)s#L%(lineno)d)"
    logging.basicConfig(format=fmt, level=logging.DEBUG)

    runner_job_id = None
    runner_id = None

    def reschedule_runner():
        nonlocal runner_job_id, runner_id

        if runner_id is not None:
            app.after_cancel(runner_id)
            runner_id = None

        job_delay = scheduler.get_seconds_until_next_job()
        if job_delay is None:
            return

        job_id, job_delay = job_delay
        delay_ms = int(job_delay * 1000)
        runner_job_id = job_id
        runner_id = app.after(delay_ms, run_job)

    def run_job():
        nonlocal runner_id
        runner_id = None

        if runner_job_id is None:
            return

        job = scheduler.get_job_by_id(runner_job_id)
        if job is not None and job.completed_at is None:
            scheduler.complete_job(job.id)
            write_to_log(f"Completed job #{job.id}")

        reschedule_runner()

    def write_to_log(message, end: str = "\n"):
        message = str(message)
        text_log.insert("end", message + end)
        text_log.see("end")

    def check_next_job():
        job_delay = scheduler.get_seconds_until_next_job()
        if job_delay is None:
            return write_to_log("No job is pending completion.")

        job_id, job_delay = job_delay
        write_to_log(f"The next job is #{job_id}, due in {job_delay:.2f} seconds.")

    def count_pending_jobs():
        n = scheduler.count_pending_jobs()
        write_to_log(f"{n} job(s) need to be completed.")

    def cleanup_jobs():
        n_completed = scheduler.delete_completed_jobs()
        n_expired = scheduler.delete_expired_jobs()
        write_to_log(
            f"{n_completed} completed job(s) and "
            f"{n_expired} expired job(s) were cleaned up."
        )

    def submit_job():
        job = scheduler.add_job_from_now(None, starts_after=3, expires_after=10)
        write_to_log(f"Submitted job #{job.id}, expected completion in 3 seconds.")
        reschedule_runner()

    enable_windows_dpi_awareness()

    with Scheduler.connect("job.db") as scheduler:
        app = Tk()
        app.geometry("600x300")
        controls = Frame(app)
        controls.pack()
        count = Button(controls, text="Check Next Job", command=check_next_job)
        count.pack(side="left")
        count = Button(controls, text="Count Pending Jobs", command=count_pending_jobs)
        count.pack(side="left")
        clear = Button(controls, text="Cleanup Jobs", command=cleanup_jobs)
        clear.pack(side="left")
        submit = Button(controls, text="Submit Job", command=submit_job)
        submit.pack(side="left")

        text_log = Text(app, font="TkDefaultFont")
        text_log.pack(fill="both", expand=True, padx=10, pady=10)

        reschedule_runner()
        app.mainloop()


if __name__ == "__main__":
    main()
