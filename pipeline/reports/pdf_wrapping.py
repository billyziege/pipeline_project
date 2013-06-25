from reportlab.lib.pagesizes import letter, inch
from reportlab.platypus import SimpleDocTemplate, Spacer, Image, Table, TableStyle

def initialize_standard_document():
    """
    Wraps the SimpleDocTemplate and returns the created doc object. 
    """
    doc = SimpleDocTemplate("form_letter.pdf",pagesize=letter,
                        rightMargin=72,leftMargin=72,
                        topMargin=72,bottomMargin=18)
