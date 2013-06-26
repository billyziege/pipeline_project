import re
from reportlab.lib.pagesizes import letter, inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from reports.post_pipeline_report import push_outliers_into_dicts, pull_five_best_concordance_matches

def initialize_standard_doc(fname):
    """
    Wraps the SimpleDocTemplate and returns the created doc object. 
    """
    doc = SimpleDocTemplate(fname,pagesize=letter,
              rightMargin=72,leftMargin=72,
              topMargin=72,bottomMargin=18)
    return doc

def add_square_images(image_files):
    """
    Creates a list of 6in x 6in images from a list of
    image files.
    """
    images = []
    for fname in image_files:
        im = Image(fname, 6*inch, 6*inch)
        images.append(im)
    return images

def outlier_table_for_pdf(config,mockdb,elements,fname,na_mark='-'):
    """
    Finds the samples that have stastics that are
    beyond the given threshold and makes a table of these statistics
    for the pdf doc object.
    """
    outliers_dicts = push_outliers_into_dicts(config,fname)
    #Set up the ouput table.
    header = ['Sample ID']
    all_sample_keys = set([])
    for column in outliers_dicts.keys():
        if len(outliers_dicts[column].keys()) > 0:
            header.append(column)
            all_sample_keys.update(set(outliers_dicts[column].keys()))
            if column == 'Concordance':
                header.append('Best matches (Concordance)')

    if len(all_sample_keys) < 1:
        return None
    styles = getSampleStyleSheet()
    elements.append(Spacer(8, 16))
    elements.append(Paragraph('<font size=16>Table 1: Outlier samples and their outlying statistics.\n</font>', styles["Normal"]))
    elements.append(Spacer(1, 8))
    data = [header]
    for sample_key in all_sample_keys:
        row = [sample_key]
        for column in header:
            if column == 'Sample ID':
                continue
            if re.search("Best matches",column):
                continue
            try:
                if column == "Concordance":
                    row.append("%.2f" % float(outliers_dicts[column][sample_key]))
                    best_matches = pull_five_best_concordance_matches(mockdb,sample_key)
                    formatted_matches = []
                    for match in best_matches:
                        formatted_matches.append(str(match[0]) + " (" + "%.2f" % float(match[1]) + ")")
                    row.append("\n".join(formatted_matches))
                elif column == "Het/Hom":
                    row.append("%.2f" % float(outliers_dicts[column][sample_key]))
                elif column == "Percentage\nin dbSNP":
                    row.append("%.2f" % float(outliers_dicts[column][sample_key]))
                else:
                    row.append(outliers_dicts[column][sample_key])
            except KeyError:
                row.append(na_mark)
                if column == "Concordance":
                    row.append(na_mark)
        data.append(row)

    t=Table(data)
    t.setStyle(TableStyle([('TEXTFONT',(0,0),(-1,-1),'Times'),
                           ('VALIGN',(0,0),(-1,-1),'MIDDLE'),
                           ('FONTSIZE',(0,0),(-1,-1),12),
                           ('ALIGN',(1,0),(-1,-1),'CENTER'),
                           ('BOX', (0,0), (-1,-1), 1, colors.black),
                           ('TEXTFONT',(0,1),(0,-1),'Times-Bold'),
                           ('ALIGN',(0,1),(0,-1),'LEFT'),
                           ('LINEBELOW', (0,0), (-1,0), 1, colors.black)
                          ]))
    elements.append(t)
    elements.append(Spacer(6, 12))
    return 1
