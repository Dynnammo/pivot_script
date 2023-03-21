from metabase_api import Metabase_API
from credentials import USERNAME, DOMAIN, PASSWORD
from utils import to_snake
import pandas as pd
import pickle
import os


filename = 'data'
env_model_id = os.getenv('ANSWER_MODEL_ID')
form_id = os.getenv('FORM_ID')
answer_model_id = env_model_id or int(
    input("Enter Model ID for the form your aiming at : ")
)
form_id = form_id or int(input("Enter ID of the form your aiming at : "))
if input("Pickling done ? Y/N / Default is Y: ") == "N":
    mtb = Metabase_API(
        DOMAIN,
        USERNAME,
        PASSWORD
    )
    res = mtb.get_card_data(card_id=answer_model_id)
    values = (
        pd.DataFrame(res)[['question_title', 'question_type', 'position']]
    ).values.tolist()
    with open(filename, 'wb') as file:
        pickle.dump(values, file)
    print("Data pickled")
else:
    print("Recover pickled data")
    with open(filename, 'rb') as file:
        values = pickle.load(file)

requests = []
sub_tables = []
extractions = []

for question_title, question_type, position in values:
    sub_table = f'"{position} answers_{to_snake(question_title)}"'
    column = f'"{position}. {question_title}"'
    sub_request = ""
    if question_type in ['short_answer', 'long_answer', 'single_option']:
        sub_request = (
            f'''{sub_table} as (
                    select *
                    from crosstab(
                        $$ select session_token, question_title, answer
                            from {{{{#{answer_model_id}}}}} _
                            where position = {position}
                                and decidim_questionnaire_id = {form_id}
                        $$,
                        $$ select distinct on (position) question_title
                            from {{{{#{answer_model_id}}}}} _
                            where position = {position}
                                and decidim_questionnaire_id = {form_id}
                        $$
                    ) as ct(
                        session_token text,
                        {column} text
                    )
                )
            '''
            )
    elif question_type in ['multiple_option']:
        sub_request = (
            f'''{sub_table} as (
                    select *
                    from crosstab(
                        $$ select session_token, question_title, array_agg(concat(answer,custom_body)) # noqa
                            from {{{{#{answer_model_id}}}}} _
                            where position = {position}
                                and decidim_questionnaire_id = {form_id}
                            group by session_token, question_title
                        $$,
                        $$ select distinct on (position) question_title
                            from {{{{#{answer_model_id}}}}} _
                            where position = {position}
                                and decidim_questionnaire_id = {form_id}
                        $$
                    ) as ct(
                        session_token text,
                        {column} text
                    )
                )
            '''
            )
    elif question_type in ['matrix_single']:
        sub_request = (
            f'''{sub_table} as (
                    select *
                    from crosstab(
                        $$  select session_token, question_title, json_object(array_agg(sub_matrix_question order by sub_matrix_question), array_agg(answer order by sub_matrix_question)) # noqa
                            from {{{{#{answer_model_id}}}}} _
                            where position = {position}
                                and decidim_questionnaire_id = {form_id}
                            group by session_token, question_title
                        $$,
                        $$ select distinct on (position) question_title
                            from {{{{#{answer_model_id}}}}} _
                            where position = {position}
                                and decidim_questionnaire_id = {form_id}
                        $$
                    ) as ct(
                        session_token text,
                        {column} json
                    )
                )
            '''
            )
    else:
        sub_request, sub_table = None, None
        print(f"Not implemented: {question_type}")

    if sub_request and sub_table:
        requests.append(sub_request)
        sub_tables.append(sub_table)
        extractions.append(f"{sub_table}.{column}")

final = [
    f'with "all_answerers" as (select distinct session_token from {{{{#{answer_model_id}}}}} _ where decidim_questionnaire_id={form_id}),', # noqa
    ",\n".join(requests)
]

final_select = (
    'select "all_answerers".session_token,' +
    " \n" +
    ',\n'.join(extractions) +
    '\n from ' +
    ' "all_answerers"'
)
for sub_table in sub_tables:
    final_select += f'\n  full join {sub_table} on {sub_table}.session_token = "all_answerers".session_token' # noqa
final.append(final_select)
final = "\n".join(final)

with open('request.sql', 'w') as file:
    file.write(final)
