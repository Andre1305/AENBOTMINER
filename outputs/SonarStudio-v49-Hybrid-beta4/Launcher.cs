using System;
using System.Diagnostics;
using System.Text;
using System.IO;
using System.Windows.Forms;
using System.Collections;
using System.Collections.Generic;

internal static class Launcher {
    [STAThread]
    private static int Main(string[] args) {
        try {
            // Some host shells supply Path and PATH simultaneously. .NET Framework
            // rejects that block when building a child's environment dictionary.
            var inherited = Environment.GetEnvironmentVariables();
            var names = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (DictionaryEntry entry in inherited) {
                string key = (string)entry.Key;
                if (!names.ContainsKey(key)) names[key] = new List<string>();
                names[key].Add(key);
            }
            foreach (var group in names.Values) {
                if (group.Count < 2) continue;
                string value = (string)inherited[group[0]];
                foreach (string name in group) Environment.SetEnvironmentVariable(name, null);
                Environment.SetEnvironmentVariable(group[0], value);
            }
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
            if (Path.GetFileNameWithoutExtension(Application.ExecutablePath).EndsWith("-Legado", StringComparison.OrdinalIgnoreCase) || Array.IndexOf(args, "--legacy") >= 0) {
                info.EnvironmentVariables["SONARSTUDIO_FORCE_LEGACY"] = "1";
            }
            if (args.Length > 0 && args[0] == "--smoke") {
                info.EnvironmentVariables["QT_QPA_PLATFORM"] = "offscreen";
                info.EnvironmentVariables["SONARSTUDIO_SCREENSHOT"] = Path.Combine(app, "launcher-test.png");
            }
            info.RedirectStandardError = true;
            info.RedirectStandardOutput = true;
            var diagnostic = new StringBuilder();
            var diagnosticLock = new object();
            Process process = Process.Start(info);
            DataReceivedEventHandler append = delegate(object sender, DataReceivedEventArgs line) {
                if (line.Data == null) return;
                lock (diagnosticLock) {
                    diagnostic.AppendLine(line.Data);
                    if (diagnostic.Length > 32768) diagnostic.Remove(0, diagnostic.Length - 32768);
                }
            };
            process.ErrorDataReceived += append;
            process.OutputDataReceived += append;
            process.BeginErrorReadLine();
            process.BeginOutputReadLine();
            if (args.Length > 0 && args[0] == "--smoke") {
                if (!process.WaitForExit(120000)) { process.Kill(); return 2; }
                process.WaitForExit();
                if (process.ExitCode != 0) File.WriteAllText(Path.Combine(app, "launcher-error.log"), "Exit code 0x" + unchecked((uint)process.ExitCode).ToString("X8") + "\n" + diagnostic.ToString());
                return process.ExitCode;
            }
            process.WaitForExit();
            if (process.ExitCode != 0) {
                string code = unchecked((uint)process.ExitCode).ToString("X8");
                string hint = code == "C06D007F" ? " Procedimento ausente em DLL: confira o runtime. Esse código não comprova falta de memória." : " Confira launcher-error.log.";
                throw new Exception("SonarStudio encerrou com código 0x" + code + "." + hint + "\n" + diagnostic.ToString());
            }
            return 0;
        } catch (Exception error) {
            File.WriteAllText(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "launcher-error.log"), error.ToString());
            if (args.Length > 0 && args[0] == "--smoke") return 1;
            MessageBox.Show(error.Message, "SonarStudio — inicialização", MessageBoxButtons.OK, MessageBoxIcon.Error);
            return 1;
        }
    }
}
