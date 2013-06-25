from reportlab.lib.pagesizes import letter, inch
from reportlab.platypus import SimpleDocTemplate, Spacer, Image, Table, TableStyle
from reports.post_pipeline_reprot import push_outliers_into_dicts

def initialize_standard_doc(fname):
    """
    Wraps the SimpleDocTemplate and returns the created doc object. 
    """
    doc = SimpleDocTemplate("fname",pagesize=letter,
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

def outlier_table_for_pdf(config,mockdb,fname,na_mark='-'):
    """
    Finds the samples that have stastics that are
    beyond the given threshold and makes a table of these statistics
    for the pdf doc object.
    """
    outlier_dicts = push_outliers_into_dicts(config,fname)
    #Set up the ouput table.
    header = ['Sample ID']
    all_sample_keys = set([])
    for column in outlier_dicts.keys():
        if len(outliers_dicts[column].keys()) > 0:
            header.append(column)
            all_sample_keys.update(set(outliers_dicts[column].keys()))
            if column == 'Concordance':
                header.append('Best matches (Concordance)')

    if len(all_sample_keys) < 1:
        return None

    data = [header]
    for sample_key in all_sample_keys:
        row = [sample_key]
        for column in header:
            if column == 'Sample ID':
                continue
            try:
                row.append(mean_depth_dict[sample_key])
                if column == "Concordance":
                    best_matches = pull_five_best_concordance_matches(mockdb,sample_key)
                    formatted_matches = []
                    for match in best_matches:
                        formatted_matches.append(str(match[0]) + " (" + str(match[1]) + ")")
                    row.append("\n".join(formatted_matches))
            except KeyError:
                row.append(na_mark)
        data.add_row(row)

    t=Table(data)
    t.setStyle(TableStyle([('FONT',(0,0),(-1,-1),'Times'),
                           ('VALIGN',(0,-1),(-1,-1),'MIDDLE'),
                           ('BOX', (0,0), (-1,-1), 1, colors.black),
                           ('FONT',(0,1),(0,-1),'Times-Bold'),
                           ('ALIGN',(0,1),(0,-1),'LEFT'),
                           ('LINEBELOW', (0,0), (-1,0), 1, colors.black)
                          ]))
    return t
