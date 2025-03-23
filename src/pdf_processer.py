import os
import time
import cv2
from paddleocr import PPStructure
from pdf2image import convert_from_path
from langchain.document_loaders import PyPDFLoader
from configs.openai_client import azure_client


# 获取当前文件的绝对路径
current_file_path = os.path.abspath(__file__)
# 获取当前文件的根目录
root_directory = os.path.dirname(os.path.dirname(current_file_path))

def _paddle_ocr_pdf(image_path):
    if not os.path.exists(image_path):
        print(f"文件 {image_path} 不存在！")
    table_engine = PPStructure(show_log=True, image_orientation=True, lang='ch')
    img = cv2.imread(image_path)
    pps_result = table_engine(img)

    content_list = []
    for type_dict in pps_result:
        if type_dict['type'] in ['title', 'text', 'header', 'figure']:
            for content_dict in type_dict['res']:
                content_list.append(content_dict['text'])

        elif type_dict['type'] == 'table':
            table_content = type_dict['res']['html']
            prompt = f"""
                我在对文档进行RAG实现对文档的问答，现在想对表格内容进行处理，我会给你一段html格式的内容，请帮我转化为一段自然语言来描述整个表格的内容, 
                形式如下：第xxx行:xxx列的值是xxx, xxx列的值是xxx；...（请自动识别表头对上面的内容进行替换）,以便后续进行embedding。如果出现缺失表头的情况，请根据你的经验适当补充。
                请直接给我转换过后的内容,不需要多余的其他内容！html内容为：{table_content}
                """
            conversation = [
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            try:
                response = azure_client.chat.completions.create(
                    model="gpt-4o",  # 使用 GPT-4 模型
                    messages=conversation,
                    temperature=0.5,  # 设置生成的温度
                    stream=False  # 启用流式输出
                )
                response_text = response.choices[0].message.content
            except:
                response_text = ''
                response = azure_client.chat.completions.create(
                    model="gpt-4o",  # 使用 GPT-4 模型
                    messages=conversation,
                    temperature=0.5,  # 设置生成的温度
                    stream=True  # 启用流式输出
                )
                for chunk in response:
                    if chunk.choices:
                        text = chunk.choices[0].delta.content
                        if text is not None:
                            response_text += text
            content_list.append(response_text)
    content_text = ''.join(content_list)
    return content_text

def pdf_process_main(pdf_file, doc_name):
    """
    pdf 文件处理主函数
    主要使用 langchain 的PyPDFLoader 读取pdf内容
    以及 pdf2image + paddleocr 的 PPStructure 的方式读取内容

    param: pdf_file pdf本都存放路径
    return:
    """
    # 使用 PyPDFLoader 加载 PDF文本内容
    loader = PyPDFLoader(pdf_file)
    pages = loader.load()

    # PaddleOCR
    # Convert PDF to images
    images = convert_from_path(pdf_path=pdf_file,
                               poppler_path=r'C:\Users\ULH2SZH\JupyterProjects\AI_Platform\use_cases\chat_with_doc\poppler-24.07.0\Library\bin'
                               )
    # Save each page as a separate image
    if not os.path.exists(os.path.join(root_directory, 'data', doc_name)):
        os.mkdir(os.path.join(root_directory, 'data', doc_name))
    for i, image in enumerate(images):
        page = pages[i]
        image_file = os.path.join(root_directory, 'data', doc_name, f"page{i + 1}.png")
        image.save(image_file, "PNG")
        ocr_content = _paddle_ocr_pdf(image_file)
        with open(os.path.join(root_directory, 'data', doc_name, f'page{i+1}_content.txt'), 'a', encoding='utf-8') as f:
            f.write(page.page_content)
            f.write('\n')
            f.write('==============================================================')
            f.write('\n')
            f.write(ocr_content)


if __name__ == '__main__':
    pdf_process_main(r"C:\Github_repo\AI_Platform\AI_Platform_ChatDoc\output\test.pdf", 'test')