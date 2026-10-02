using System;
using System.Diagnostics;
using System.IO;
using System.Windows.Forms;

internal static class Launcher {
    [STAThread]
    private static int Main(string[] args) {
        try {
            string app = AppDomain.CurrentDomain.BaseDirectory;
            string root = Path.GetFullPath(Path.Combine(app, "..", ".."));
            string prefix = Path.Combine(root, "work", "PINGMapper-main", ".pixi", "envs", "default");
            string python = Path.Combine(prefix, "pythonw.exe");
            if (!File.Exists(python)) throw new FileNotFoundException("Não encontrei o ambiente Python. Preserve a pasta work ao lado de outputs.", python);
            var info = new ProcessStartInfo(python, "\"" + Path.Combine(app, "app.py") + "\"");
            info.WorkingDirectory = app;
            info.UseShellExecute = false;
            info.CreateNoWindow = true;
            info.EnvironmentVariables["PATH"] = Path.Combine(prefix, "Library", "bin") + ";" + Path.Combine(prefix, "Scripts") + ";" + prefix + ";" + Environment.GetEnvironmentVariable("PATH");
            info.EnvironmentVariables["CONDA_PREFIX"] = prefix;
            info.EnvironmentVariables["GDAL_DATA"] = Path.Combine(prefix, "Library", "share", "gdal");
            info.EnvironmentVariables["PROJ_DATA"] = Path.Combine(prefix, "Library", "share", "proj");
            info.EnvironmentVariables["PYTHONPATH"] = Path.Combine(root, "work");
            info.EnvironmentVariables["TEMP"] = Path.Combine(root, "work", "tmp");
            info.EnvironmentVariables["TMP"] = Path.Combine(root, "work", "tmp");
            info.EnvironmentVariables["MPLCONFIGDIR"] = Path.Combine(root, "work", "mpl");
            info.EnvironmentVariables["NUMBA_CACHE_DIR"] = Path.Combine(root, "work", "numba-cache");
            if (args.Length > 0 && args[0] == "--smoke") {
                info.EnvironmentVariables["QT_QPA_PLATFORM"] = "offscreen";
                info.EnvironmentVariables["SONARSTUDIO_SCREENSHOT"] = Path.Combine(app, "launcher-test.png");
            }
            Process process = Process.Start(info);
            if (args.Length > 0 && args[0] == "--smoke") {
                if (!process.WaitForExit(120000)) return 2;
                return process.ExitCode;
            }
            return 0;
        } catch (Exception error) {
            MessageBox.Show(error.Message, "SonarStudio — inicialização", MessageBoxButtons.OK, MessageBoxIcon.Error);
            return 1;
        }
    }
}
