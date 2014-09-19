"""Quality control and summary metrics for next-gen alignments and analysis.
"""
import contextlib
import copy
import csv
import glob
import os
import sys
import subprocess
import xml.etree.ElementTree as ET

import argparse
import yaml
from mako.template import Template
import pysam

from bcbio import utils
from bcbio.broad import runner_from_config
from bcbio.broad.metrics import PicardMetrics, PicardMetricsParser, RNASeqPicardMetrics
from bcbio.log import logger
from bcbio.pipeline import config_utils

# ## High level functions to generate summary PDF

def variant_align_summary(bam_file, sam_ref, bait_file, sample_name, config, output_dir):
    """Run alignment summarizing script to produce a pdf with align details.
    """
    with utils.curdir_tmpdir() as tmp_dir:
        graphs, summary, overrep = \
                _graphs_and_summary(bam_file, sam_ref, bait_file, output_dir, tmp_dir, config)
    with utils.chdir(output_dir):
        return {"pdf": _generate_pdf(graphs, summary, overrep, bam_file, sample_name,
                                     output_dir, config),
                "metrics": summary}

def _safe_latex(to_fix):
    """Escape characters that make LaTeX unhappy.
    """
    chars = ["%", "_", "&", "#"]
    for char in chars:
        to_fix = to_fix.replace(char, "\\%s" % char)
    return to_fix

def _generate_pdf(graphs, summary, overrep, bam_file, sample_name,
                  output_dir, config):
    base = os.path.splitext(os.path.basename(bam_file))[0]
    sample_name = base if sample_name is None else " : ".join(sample_name)
    tmpl = Template(_section_template)
    sample_name = "%s (%s)" % (_safe_latex(sample_name),
                               _safe_latex(base))
    section = tmpl.render(name=sample_name, summary=None,
                          summary_table=summary,
                          figures=[(f, c, i) for (f, c, i) in graphs if f],
                          overrep=overrep)
    out_file = os.path.join(output_dir, "%s-summary.tex" % base)
    out_tmpl = Template(_base_template)
    with open(out_file, "w") as out_handle:
        out_handle.write(out_tmpl.render(parts=[section]))
    pdf_file = "%s.pdf" % os.path.splitext(out_file)[0]
    if not utils.file_exists(pdf_file):
        cl = [config_utils.get_program("pdflatex", config), out_file]
        subprocess.check_call(cl)
    return pdf_file

def _graphs_and_summary(bam_file, sam_ref, bait_file, output_dir, tmp_dir, config):
    """Prepare picard/FastQC graphs and summary details.
    """
    broad_runner = runner_from_config(config)
    metrics = PicardMetrics(broad_runner, tmp_dir)
    summary_table, metrics_graphs = \
                   metrics.report(bam_file, sam_ref, is_paired(bam_file),
                                  bait_file,
                                  bait_file,
                                  False,
                                  config)
    metrics_graphs = [(p, c, 0.75) for p, c in metrics_graphs]
    fastqc_graphs, fastqc_stats, fastqc_overrep = \
                   fastqc_report(bam_file, output_dir, config)
    all_graphs = fastqc_graphs + metrics_graphs
    summary_table = _update_summary_table(summary_table, sam_ref, fastqc_stats)
    return all_graphs, summary_table, fastqc_overrep

def _update_summary_table(summary_table, ref_file, fastqc_stats):
    stats_want = []
    summary_table[0] = (summary_table[0][0], summary_table[0][1],
            "%sbp %s" % (fastqc_stats.get("Sequence length", "0"), summary_table[0][-1]))
    for stat in stats_want:
        summary_table.insert(0, (stat, fastqc_stats.get(stat, ""), ""))
    ref_org = os.path.splitext(os.path.split(ref_file)[-1])[0]
    summary_table.insert(0, ("Reference organism",
        ref_org.replace("_", " "), ""))
    return summary_table

def is_paired(bam_file):
    """Determine if a BAM file has paired reads.
    """
    with contextlib.closing(pysam.Samfile(bam_file, "rb")) as in_pysam:
        for read in in_pysam:
            return read.is_paired

def fastqc_report(bam_file, qc_dir, config):
    """Calculate statistics about a read using FastQC.
    """
    out_dir = _run_fastqc(bam_file, qc_dir, config)
    parser = FastQCParser(out_dir)
    graphs = parser.get_fastqc_graphs()
    stats, overrep = parser.get_fastqc_summary()
    return graphs, stats, overrep

class FastQCParser:
    def __init__(self, base_dir):
        self._dir = base_dir
        self._max_seq_size = 45
        self._max_overrep = 20

    def get_fastqc_graphs(self):
        graphs = (("per_base_quality.png", "", 1.0),
                  ("per_base_sequence_content.png", "", 0.85),
                  ("per_sequence_gc_content.png", "", 0.85),
                  ("kmer_profiles.png", "", 0.85),)
        final_graphs = []
        for f, caption, size in graphs:
            full_f = os.path.join(self._dir, "Images", f)
            if os.path.exists(full_f):
                final_graphs.append((full_f, caption, size))
        return final_graphs

    def get_fastqc_summary(self):
        stats = {}
        for stat_line in self._fastqc_data_section("Basic Statistics")[1:]:
            k, v = [_safe_latex(x) for x in stat_line.split("\t")[:2]]
            stats[k] = v
        over_rep = []
        for line in self._fastqc_data_section("Overrepresented sequences")[1:]:
            parts = [_safe_latex(x) for x in line.split("\t")]
            over_rep.append(parts)
            over_rep[-1][0] = self._splitseq(over_rep[-1][0])
        return stats, over_rep[:self._max_overrep]

    def _splitseq(self, seq):
        pieces = []
        cur_piece = []
        for s in seq:
            if len(cur_piece) >= self._max_seq_size:
                pieces.append("".join(cur_piece))
                cur_piece = []
            cur_piece.append(s)
        pieces.append("".join(cur_piece))
        return " ".join(pieces)

    def _fastqc_data_section(self, section_name):
        out = []
        in_section = False
        data_file = os.path.join(self._dir, "fastqc_data.txt")
        if os.path.exists(data_file):
            with open(data_file) as in_handle:
                for line in in_handle:
                    if line.startswith(">>%s" % section_name):
                        in_section = True
                    elif in_section:
                        if line.startswith(">>END"):
                            break
                        out.append(line.rstrip("\r\n"))
        return out

def _run_fastqc(bam_file, qc_dir, config):
    out_base = utils.safe_makedir(os.path.join(qc_dir, "fastqc"))
    fastqc_out = os.path.join(out_base, "%s_fastqc" %
                              os.path.splitext(os.path.basename(bam_file))[0])
    if not os.path.exists(fastqc_out):
        cl = [config_utils.get_program("fastqc", config),
              "-o", out_base, "-f", "bam", bam_file]
        subprocess.check_call(cl)
    if os.path.exists("%s.zip" % fastqc_out):
        os.remove("%s.zip" % fastqc_out)
    return fastqc_out

# ## LaTeX templates for output PDF

_section_template = r"""
\subsection*{${name}}

% if summary_table:
    \begin{table}[h]
    \centering
    \begin{tabular}{|l|rr|}
    \hline
    % for label, val, extra in summary_table:
        %if label is not None:
            ${label} & ${val} & ${extra} \\%
        %else:
            \hline
        %endif
    %endfor
    \hline
    \end{tabular}
    \caption{Summary of lane results}
    \end{table}
% endif

% if summary:
    \begin{verbatim}
    ${summary}
    \end{verbatim}
% endif

% for i, (figure, caption, size) in enumerate(figures):
    \begin{figure}[htbp]
      \centering
      \includegraphics[width=${size}\linewidth] {${figure}}
      \caption{${caption}}
    \end{figure}
% endfor

% if len(overrep) > 0:
    \begin{table}[htbp]
    \centering
    \begin{tabular}{|p{8cm}rrp{4cm}|}
    \hline
    Sequence & Count & Percent & Match \\%
    \hline
    % for seq, count, percent, match in overrep:
        \texttt{${seq}} & ${count} & ${"%.2f" % float(percent)} & ${match} \\%
    % endfor
    \hline
    \end{tabular}
    \caption{Overrepresented read sequences}
    \end{table}
% endif

\FloatBarrier
"""

_base_template = r"""
\documentclass{article}
\usepackage{fullpage}
\usepackage{graphicx}
\usepackage{placeins}

\begin{document}
% for part in parts:
    ${part}
% endfor
\end{document}
"""

if __name__ == '__main__':
    #Handle arguments
    parser = argparse.ArgumentParser(description='Generate the summary pdf (as in earlier versions of the pipeline.)')
    parser.add_argument('bam_file', type=str, help='The analysis ready bam file.')
    parser.add_argument('fasta_ref', type=str, help='The reference fasta file.')
    parser.add_argument('bait_file', type=str, help='The bed file detailing the bait/target region.')
    parser.add_argument('sample_name', type=str, help='The name of the sample to appear in the summary.')
    parser.add_argument('config_file', type=str, help='The system configuration file used in the bcbio pipeline.')
    parser.add_argument('output_dir', type=str, help='The directory where the output will be written.')
    args = parser.parse_args()
  

    config = config_utils.load_config(args.config_file)
    variant_align_summary(args.bam_file, args.fasta_ref, args.bait_file, args.sample_name, config, args.output_dir)
