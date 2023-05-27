import wget
import os
import io
import gzip
import tarfile
import pandas as pd
import pickle
from luigi import Task, run, LocalTarget, Parameter, build, Config


class GlobalParams(Config):
    dataset = Parameter(default='gse68849')


def bar_custom(current, total, width=80):
    print("Downloading: %d%% [%d / %d] bytes" % (current / total * 100, current, total))


class DownloadDataset(Task):
    # dataset = Parameter()

    def run(self):
        if not os.path.exists('data'):
            os.makedirs(os.path.join('data', GlobalParams().dataset))

        url = f'https://www.ncbi.nlm.nih.gov/geo/download/?acc={GlobalParams().dataset}&format=file'
        wget.download(url, f'data/{GlobalParams().dataset}/{GlobalParams().dataset}_RAW.tar', bar=bar_custom)

    def output(self):
        return LocalTarget(f'data/{GlobalParams().dataset}/{GlobalParams().dataset}_RAW.tar')


def delete_column(file):
    df = pd.read_csv(file, sep="\t")
    df = df.drop(columns=['Definition', 'Ontology_Component',
                          'Ontology_Process', 'Ontology_Function',
                          'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence'])
    new_file = f'{file[:-4]}_modified.tsv'
    df.to_csv(new_file, sep="\t")


def get_dataframe(file: str) -> dict:
    dfs = {}
    with open(file) as f:
        write_key = None
        fio = io.StringIO()
        for l in f.readlines():
            if l.startswith('['):
                if write_key:
                    fio.seek(0)
                    header = None if write_key == 'Heading' else 'infer'
                    dfs[write_key] = pd.read_csv(fio, sep='\t', header=header)
                fio = io.StringIO()
                write_key = l.strip('[]\n')
                continue
            if write_key:
                fio.write(l)
        fio.seek(0)
        dfs[write_key] = pd.read_csv(fio, sep='\t')
    return dfs


def create_tsv(file, folder):
    dfs = get_dataframe(file)
    for key in dfs.keys():
        df = dfs.get(key)
        file_to_save = f'{folder}/{key}.tsv'
        df.to_csv(file_to_save, sep="\t")


class UnZip(Task):
    # dataset = Parameter()

    def output(self):
        return LocalTarget(f'data/{GlobalParams().dataset}/unzip_filelist.lst')

    def run(self):
        archive = f'data/{GlobalParams().dataset}/{GlobalParams().dataset}_RAW.tar'

        with tarfile.open(archive, 'r') as tar:
            for subfile in tar.getmembers():
                if subfile.name.endswith('.gz'):
                    with gzip.open(tar.extractfile(subfile)) as gz:
                        folder_name = f'data/{GlobalParams().dataset}/' + os.path.basename(subfile.name)[:-7]
                        os.makedirs(folder_name, exist_ok=True)
                        extract_file = os.path.join(folder_name, os.path.basename(subfile.name)[:-3])
                        with open(extract_file, "wb") as f:
                            f.write(gz.read())

        with self.output().open('w') as output:
            for root, dirs, files in os.walk('./data'):
                for fn in [os.path.join(root, name) for name in files]:
                    output.write("%s\n" % fn)

    def requires(self):
        return DownloadDataset()


class CreateTSV(Task):
    # dataset = Parameter()

    def requires(self):
        return UnZip()

    def output(self):
        return LocalTarget(f'data/{GlobalParams().dataset}/tsv_files.lst')

    def run(self):
        for root, dirs, files in os.walk('./data'):
            for file in files:
                if file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    create_tsv(file=file_path, folder=root)

        with self.output().open('w') as output:
            for root, dirs, files in os.walk('./data'):
                for fn in [os.path.join(root, name) for name in files if name.endswith('.tsv')]:
                    output.write("%s\n" % fn)


class ModifyProbes(Task):
    # dataset = Parameter()

    def requires(self):
        return CreateTSV()

    def output(self):
        return LocalTarget(f'data/{GlobalParams().dataset}/modified_probe_files.lst')

    def run(self):
        name = 'Probes.tsv'
        for root, dirs, files in os.walk('./data'):
            if name in files:
                file = os.path.join(root, name)
                delete_column(file)

        with self.output().open('w') as output:
            for root, dirs, files in os.walk('./data'):
                for fn in [os.path.join(root, name) for name in files if name.endswith('_modified.tsv')]:
                    output.write("%s\n" % fn)


class DeleteSourceTXT(Task):
    # dataset = Parameter()

    def requires(self):
        return ModifyProbes()

    def output(self):
        return LocalTarget(f'data/{GlobalParams().dataset}/deleted_source_txt.lst')

    def run(self):

        with self.output().open('w') as output:
            for root, dirs, files in os.walk('./data'):
                for fn in [os.path.join(root, name) for name in files if name.endswith('.txt')]:
                    output.write("%s\n" % fn)
                    os.remove(fn)


if __name__ == '__main__':
    luigi_run_result = build([
        DownloadDataset(),
        UnZip(),
        CreateTSV(),
        ModifyProbes(),
        DeleteSourceTXT()
           ])

