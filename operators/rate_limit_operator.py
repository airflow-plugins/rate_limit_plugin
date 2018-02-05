import logging

from airflow.models import BaseOperator, SkipMixin
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_email

from MarketoPlugin.hooks.marketo_hook import MarketoHook


class RateLimitOperator(BaseOperator, SkipMixin):
    """
    Rate Limit Operator
    :param service:                 The relevant service to check the rate limit
                                    against. Possible values include:
                                        - marketo
    :type service:                  string
    :param service_conn_id:         The Airflow connection id used to store
                                    the relevant service credentials.
    :type service_conn_id:          string
    :param threshold:               The threshold to trigger the operator to
                                    skip downstram tasks. This can be either a
                                    decimal, representing a percentage, or an
                                    integer, representing a total request count.
    :param threshold:               float/integer
    :param threshold_type:          The type of threshold that is being used.
                                    Possible values for this include:
                                        - percentage
                                        - count
                                    By default, this is set to "percentage".
    :type threshold_type:           string
    :param total_request_override:  *(optional)* Each service will have a
                                    default total request limit as provided
                                    by the service's documentation. This
                                    parameter will override this default limit
                                    and be used as the ceiling against which
                                    the threshold is compared.
    :param total_request_override:  integer
    :param email_to:                *(optional)* If the threshold has been hit,
                                    send an email to the specified email(s).
                                    Multiple email addresses may be specified
                                    by a list.
    :type email_to:                 string/list
    """

    @apply_defaults
    def __init__(self,
                 service,
                 service_conn_id,
                 threshold,
                 threshold_type='percentage',
                 total_request_override=False,
                 email_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.service = service
        self.service_conn_id = service_conn_id
        self.threshold = threshold
        self.threshold_type = threshold_type
        self.total_request_override = total_request_override
        self.email_to = email_to

        if self.service.lower() not in ('marketo'):
            raise Exception('Specified service is not currently supported for rate limit check.')

        if self.threshold_type not in ('percentage', 'count'):
            raise Exception('Please choose "percentage" or "count" for threshold_type.')

    def execute(self, context):
        condition, current_request_count = self.service_mapper()

        if condition:
            self.condition_met(current_request_count)
        else:
            self.condition_not_met(current_request_count, context)

    def condition_met(self, current_request_count):
        logging.info('Rate Limit has not been exceeded.')
        logging.info('Current request count is: {}'
                      .format(str(current_request_count)))
        logging.info('Proceeding with downstream tasks...')

    def condition_not_met(self, current_request_count, context):
        logging.info('Rate Limit has been exceeded.')
        logging.info('Current request count is: {}'
                      .format(str(current_request_count)))
        logging.info('The specified threshold was: {}'
                      .format(str(self.threshold)))
        logging.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        logging.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'],
                      context['ti'].execution_date,
                      downstream_tasks)

        logging.info('Sending email reminder to retry.')

        if self.email_to is not None:
            self.send_email(context)

        logging.info("Marking task as complete.")

    def service_mapper(self):
        mapper = {'marketo': self.marketo_check(),
                  'hubspot': self.salesforce_check(),
                  'salesforce': self.hubspot_check()}

        return mapper[self.service]

    def send_email(self, context):
        email_subject = "Rate Limit Hit for: {0}".format(self.service)

        html_content = \
            """
            The rate limit has been hit in "{0}" for the following service: {1}.

            Because the rate limit operator has been used, all downstream tasks
            have been skipped.

            Please inspect the relevant DAG and re-run the task at a later time.
            """.format(context['dag_run'], self.service)

        send_email(self.email_to, email_subject, html_content)

        logging.info("Email sent to: {}.".format(self.email_to))

    def marketo_check(self):
        token = (MarketoHook(http_conn_id=self.service_conn_id)
                 .run('identity/oauth/token')
                 .json())['access_token']

        # First check the API Usage too see that we are not approaching a
        # threshold based on the input parameters. 50000 requests/day is the
        # limit Marketo has set.
        hook = MarketoHook(http_conn_id=self.service_conn_id)
        usage = hook.run('rest/v1/stats/usage.json', token=token).json()
        total_current_requests = usage['result'][0]['total']

        if self.threshold_type == 'percentage':
            denominator = self.total_request_override if self.total_request_override else 50000

            usage_percentage = total_current_requests/denominator

            if usage_percentage > self.threshold:
                return False, total_current_requests
            else:
                return True, total_current_requests

        elif self.threshold_type == 'count':
            if total_current_requests > self.threshold:
                return False, total_current_requests
            else:
                return True, total_current_requests

    def salesforce_check(self):
        logging.info('Support for Salesforce has yet to be implemented.')
        pass

    def hubspot_check(self):
        logging.info('Support for Hubspot has yet to be implemented.')
        pass
