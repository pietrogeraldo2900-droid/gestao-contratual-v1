from __future__ import annotations

import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext

from main import process_text_file


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title('Sistema de Relatórios de Obra - Interface')
        self.root.geometry('900x650')
        self.current_file: Path | None = None

        top = tk.Frame(root)
        top.pack(fill='x', padx=12, pady=10)

        tk.Button(top, text='Abrir TXT', command=self.open_txt, width=16).pack(side='left', padx=4)
        tk.Button(top, text='Salvar texto como TXT', command=self.save_txt, width=18).pack(side='left', padx=4)
        tk.Button(top, text='Processar texto', command=self.process_current, width=16).pack(side='left', padx=4)

        self.path_var = tk.StringVar(value='Nenhum arquivo selecionado')
        tk.Label(root, textvariable=self.path_var, anchor='w').pack(fill='x', padx=12)

        tk.Label(root, text='Cole aqui a mensagem do WhatsApp ou abra um arquivo .txt', anchor='w').pack(fill='x', padx=12, pady=(8, 4))
        self.editor = scrolledtext.ScrolledText(root, wrap='word', font=('Consolas', 10))
        self.editor.pack(fill='both', expand=True, padx=12, pady=(0, 10))

        bottom = tk.Frame(root)
        bottom.pack(fill='x', padx=12, pady=(0, 12))
        tk.Label(bottom, text='Saída padrão: pasta ao lado do TXT com prefixo saida_').pack(side='left')

    def open_txt(self):
        filename = filedialog.askopenfilename(filetypes=[('Arquivo TXT', '*.txt'), ('Todos os arquivos', '*.*')])
        if not filename:
            return
        path = Path(filename)
        self.current_file = path
        self.path_var.set(str(path))
        self.editor.delete('1.0', tk.END)
        self.editor.insert(tk.END, path.read_text(encoding='utf-8'))

    def save_txt(self):
        filename = filedialog.asksaveasfilename(defaultextension='.txt', filetypes=[('Arquivo TXT', '*.txt')])
        if not filename:
            return
        path = Path(filename)
        path.write_text(self.editor.get('1.0', tk.END).strip() + '\n', encoding='utf-8')
        self.current_file = path
        self.path_var.set(str(path))
        messagebox.showinfo('Salvo', f'Texto salvo em:\n{path}')

    def process_current(self):
        text = self.editor.get('1.0', tk.END).strip()
        if not text:
            messagebox.showwarning('Sem conteúdo', 'Cole um texto ou abra um arquivo TXT antes de processar.')
            return
        if self.current_file is None:
            temp_path = Path.cwd() / 'entrada_interface.txt'
            temp_path.write_text(text + '\n', encoding='utf-8')
            self.current_file = temp_path
            self.path_var.set(str(temp_path))
        else:
            self.current_file.write_text(text + '\n', encoding='utf-8')

        try:
            output_dir = process_text_file(self.current_file, self.current_file.parent / f'saida_{self.current_file.stem}')
        except Exception as exc:
            messagebox.showerror('Erro ao processar', str(exc))
            return
        messagebox.showinfo('Concluído', f'Saída gerada em:\n{output_dir}')


def main():
    root = tk.Tk()
    App(root)
    root.mainloop()


if __name__ == '__main__':
    main()
