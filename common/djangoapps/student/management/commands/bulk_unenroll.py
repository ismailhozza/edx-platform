from __future__ import absolute_import

import logging

import unicodecsv
from django.core.exceptions import ObjectDoesNotExist
from django.core.management.base import BaseCommand
from django.db.models import Q
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey

from student.models import CourseEnrollment, User, BulkUnenrollConfiguration

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class Command(BaseCommand):

    help = """"
    Un-enroll bulk users from the courses.
    It expect that the data will be provided in a csv file format with
    first row being the header and columns will be as follows:
    user_id, username, email, course_id, is_verified, verification_date
    """

    def add_arguments(self, parser):
        parser.add_argument('-p', '--csv_path',
                            metavar='csv_path',
                            dest='csv_path',
                            required=False,
                            help='Path to CSV file.')

    def handle(self, *args, **options):

        csv_path = options['csv_path']
        if csv_path:
            with open(csv_path) as csv_file:
                self.un_enroll_users(csv_file)
        else:
            csv_file = BulkUnenrollConfiguration.current().csv_file
            self.un_enroll_users(csv_file)

    def un_enroll_users(self, csv_file):
        reader = list(unicodecsv.DictReader(csv_file))
        users = self.get_users_info(reader)
        enrollments, course_ids = self.get_enrollments(reader, users)
        users_unenrolled = []
        for row in reader:
            username = row['username']
            email = row['email']
            course_key = row['course_id']
            try:
                user = users.get(Q(username=username) | Q(email=email))
            except ObjectDoesNotExist:
                user = None
                msg = 'User with username {} or email {} does not exist'.format(username, email)
                logger.warning(msg)

            try:
                course_id = course_ids[course_key]
            except KeyError:
                course_id = None
                msg = 'Invalid course id {course_id}, skipping un-enrollement for {username}, {email}'.format(**row)
                logger.warning(msg)

            try:
                enrollments.get(user_id=user.id, course_id=course_id)
                try:
                    CourseEnrollment.unenroll(user, course_id, skip_refund=True)
                    users_unenrolled.append("{username}:{course_id}".format(**row))
                except Exception as err:
                    msg = 'Error un-enrolling User {} from course {}: '.format(username, course_key, err)
                    logger.error(msg, exc_info=True)
            except (ObjectDoesNotExist, AttributeError):
                msg = 'Enrollment for the user {} in course {} does not exist!'.format(username, course_key)
                logger.info(msg)

        logger.info("Following users has been unenrolled successfully from the following courses: {users_unenrolled}"
                    .format(users_unenrolled=users_unenrolled))

    def get_users_info(self, reader):

        user_names = [row['username'] for row in reader]
        emails = [row['email'] for row in reader]
        users = User.objects.filter(Q(username__in=user_names) | Q(email__in=emails))
        return users

    def get_enrollments(self, reader, users):

        course_ids = {}
        for row in reader:
            try:
                course_id = CourseKey.from_string(row['course_id'])
                course_ids[row['course_id']] = course_id
            except InvalidKeyError:
                msg = 'Invalid course id {course_id}'.format(**row)
                logger.warning(msg)

        enrollments = CourseEnrollment.objects.filter(user__in=users, course_id__in=course_ids.values())
        return enrollments, course_ids

