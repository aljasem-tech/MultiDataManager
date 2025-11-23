import ast
import os
import shutil
from typing import Dict, List, Tuple

from multi_data_manager.core.logger import logger


class DocumentationGenerator:
    """
    Generates markdown documentation from Python source code.
    """

    def __init__(self, root_dir: str, destination_dir: str, read_me_path: str = 'README.md'):
        self.root_dir = root_dir
        self.destination_dir = destination_dir
        self.read_me_path = read_me_path

    def generate_docs(self):
        """
        Walks through the root directory and generates docs for each Python file.
        """
        if not os.path.exists(self.destination_dir):
            os.makedirs(self.destination_dir)

        for root, _, files in os.walk(self.root_dir):
            for file in files:
                if file.endswith('.py') and not file.endswith('__init__.py'):
                    file_path = os.path.join(root, file)
                    self._generate_module_docs(file_path)

    def copy_readme_to_docs(self):
        """
        Copies the project README to the documentation directory.
        """
        if os.path.exists(self.read_me_path):
            # Assuming docs structure is docs/auto_generated_docs, so ../README.md puts it in docs/
            destination_readme_path = os.path.join(self.destination_dir, '../', 'README.md')
            # Ensure parent dir exists
            os.makedirs(os.path.dirname(destination_readme_path), exist_ok=True)

            shutil.copyfile(self.read_me_path, destination_readme_path)
            logger.info(f'Copied {self.read_me_path} to {destination_readme_path}')

    def _generate_module_docs(self, file_path: str):
        try:
            file_name = os.path.basename(file_path).split('.')[0]
            output_path = os.path.join(self.destination_dir, f'{file_name}.md')
            with open(file_path, 'r', encoding='utf-8') as file:
                tree = ast.parse(file.read(), filename=file_path)

            doc_content = []
            rel_path = os.path.relpath(file_path, os.path.dirname(self.root_dir))
            relative_file_path = os.path.splitext(rel_path)[0].replace(os.path.sep, '.')

            class_methods, standalone_functions = self._get_class_and_function_names(tree)
            for class_info in class_methods:
                doc_content.extend(self._generate_class_documentation(relative_file_path, class_info))
            for function in standalone_functions:
                doc_content.append(f'::: {relative_file_path}.{function}')

            if doc_content:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(doc_content))
                logger.info(f'Documentation generated: {output_path}')

        except ImportError as e:
            logger.error(f'Error importing module {file_path}: {e}')
        except Exception as e:
            logger.error(f'Error processing module {file_path}: {e}')

    @staticmethod
    def _generate_class_documentation(relative_file_path: str, class_info: Dict) -> List[str]:
        doc_content = []
        main_class_name = class_info['name']
        doc_content.append(f'::: {relative_file_path}.{main_class_name}')

        for nested_class in class_info['nested_classes']:
            nested_class_name = nested_class['name']
            doc_content.append(f'::: {relative_file_path}.{main_class_name}.{nested_class_name}')

        return doc_content

    @staticmethod
    def _get_class_and_function_names(tree: ast.AST) -> Tuple[List[Dict], List[str]]:
        class_info = []
        standalone_functions = []

        def process_class(_node: ast.ClassDef):
            nested_classes = []

            for item in _node.body:
                if isinstance(item, ast.ClassDef):
                    nested_classes.append(process_class(item))

            return {
                'name': _node.name,
                'nested_classes': nested_classes
            }

        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.ClassDef):
                class_info.append(process_class(node))
            elif isinstance(node, ast.FunctionDef):
                standalone_functions.append(node.name)

        return class_info, standalone_functions
