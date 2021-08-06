from adapter import BaseAdapter
from models import Student, Course
from adapter import Column

class StudentAdapter(BaseAdapter):
    non_data_rows = [
        ['This could be a header row with text at top of Excel file providing context.'], 
        ['Or explain to how this plugin works for users.']]
    model = Student
    columns = {
        'id': Column(header='DBID'),
        'student_id': Column(
            header='Student ID',
            extractor=lambda r: f'{r.project.jira_link.split("/")[-1]}-{r.jira_number}' if r.jira_number else '',
            inserter=lambda _as, dbid, v: _as.add(Student, dbid, jira_number=int(v.split('-')[-1]) if v.split('-')[-1] else None)
        ),
        'first name': Column(
            header='First Name',
            extractor=lambda r: r.first_name,
            inserter=None
        ),
        'last name': Column(
            header='Last Name',
            extractor=lambda r: r.last_name,
            inserter=None
        ),
        'join_date': Column(
            header='join Date',
            extractor=None,
            inserter=None
        ),
        'GPA': Column(
            header='Overall GPA',
            extractor=lambda r: str(sum([rc.grade for rc in r.courses.all()])/len(r.courses.all())),
            inserter=None
        ),
        'ban student': Column(
            header='Student is banned?',       
            extractor=lambda r: r.banned,
            inserter=lambda _as, dbid, v: StudentAdapter.unenroll_student(_as, dbid)
        ),
        'label': Column(header='Label'),
        'delete_tag': Column(
            header='Deletion Tag',
            inserter=lambda _as, dbid, v: _as.delete(Student, dbid)
        ),

        def unenroll_student(adapter_staging: AdapterStaging, dbid: str):
            if not adapter_staging.has_commit_runnable(AdapterStaging.PRECOMMIT_UPDATE_LAMBDA, dbid):
                adapter_staging.add_commit_runnable(AdapterStaging.PRECOMMIT_UPDATE_LAMBDA, dbid, lambda data, row: StudentAdapter.unenroll(data, row))
            
        def unenroll_course(data, row):
            """Unenroll's a student from a single course

            :param data: The update data for the row
            :type data: dict
            :param row: The Student row being unenrolled
            :type row: Model
            :return: The same data
            :rtype: dict
            """
            course.unenroll() for course in row.courses.all()
            row.gpa = 0