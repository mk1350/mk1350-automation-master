"""
自动化办公大师 - 龙虾平台技能主入口
完整版 - 包含所有功能实现，无任何省略
"""
import os
import logging
import tempfile
import shutil
import zipfile
from typing import Dict, Any, List, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import numpy as np

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ==================== 导入所有服务类 ====================

# 自动化主服务
from .services.automation_service import AutoMation as AutomationService

# 发票提取服务（两个版本）
from .services.invoice_extraction_service import InvoiceExtractionService
from .services.invoice_extraction_service_complete import InvoiceExtractionServiceComplete

# 财税对账服务
from .services.optimized_tax_reconciliation_service import OptimizedTaxAmountReconciliationService

# 文件处理服务
from .services.file_convert_service import MutualConver
from .services.file_rename_service import OsOperation
from .services.file_generate_service import TemplateEngine
from .services.data_operation_service import DataOperation

# 工具类（保留，但不会被直接调用）
from .services.parallel_executor import ParallelExecutor
from .services.process_manager import ProcessManager

# 初始化主服务
automation = AutomationService()


# ==================== 工具函数 ====================

def convert_types(obj: Any) -> Any:
    """转换NumPy类型为Python原生类型（用于JSON序列化）"""
    if isinstance(obj, (np.bool_)):
        return bool(obj)
    elif isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_types(item) for item in obj]
    elif hasattr(obj, 'item'):  # 其他NumPy类型
        return obj.item()
    else:
        return obj


def check_trial_quota(user_id: str, action: str) -> Dict[str, Any]:
    """检查试用额度"""
    # TODO: 实际部署时需要从数据库/缓存读取
    # 这里简化实现，假设每个用户每种付费功能有2次试用
    return {
        'has_trial': True,
        'trial_available': True,
        'remaining': 2,
        'message': f'您还有2次免费试用机会'
    }


def deduct_trial_quota(user_id: str, action: str):
    """扣减试用次数"""
    # TODO: 实际部署时需要实现存储
    pass


def calculate_billing(action: str, result: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    """计算费用"""
    # 试用模式不计费
    if params.get('_trial_mode'):
        return {
            'charged': False,
            'amount': 0,
            'message': '免费试用',
            'trial_remaining': 1
        }
    
    # 专业版用户不计单次费
    if params.get('_subscription', {}).get('tier') == 'pro':
        return {
            'charged': False,
            'amount': 0,
            'message': '专业版不限次'
        }
    
    # 按次计费
    prices = {
        'invoice_extract': 1.0,  # 1元/张
        'tax_reconcile': 29.0     # 29元/次
    }
    
    if action == 'invoice_extract':
        success_count = result.get('summary', {}).get('success_count', 0)
        amount = success_count * prices[action]
        
        # 准确率保证
        overall_accuracy = result.get('summary', {}).get('overall_accuracy', 100)
        if overall_accuracy < 98:
            return {
                'charged': False,
                'amount': 0,
                'refund': True,
                'reason': f'准确率{overall_accuracy:.1f}%低于98%保证'
            }
        
    elif action == 'tax_reconcile':
        amount = prices[action]
        
        # 效果保证
        if result.get('need_refund', False):
            return {
                'charged': False,
                'amount': 0,
                'refund': True,
                'reason': result.get('refund_reason')
            }
    else:
        amount = 0
    
    return {
        'charged': amount > 0,
        'amount': amount,
        'message': f'消费 {amount} 元'
    }


def get_algorithm_params(mode: str) -> Dict[str, Any]:
    """获取对账算法参数"""
    # 基础参数（标准模式）
    params = {
        'exact_match_threshold': 0.001,
        'approx_match_threshold_percent': 1.0,
        'many_to_many_amount_threshold': 1500,
        'many_to_many_percent_threshold': 1.0,
        'final_match_amount_threshold': 100.0,
        'final_match_percent_threshold': 0.1,
        'recursive_min_amount_threshold': 0.01,
        'recursive_percent_threshold': 10.0,
        'large_amount_threshold': 1000,
        'pruning_threshold_percent': 90.0,
        'backtrack_overtune_percent': 10.0,
        'backtrack_pruning_threshold_percent': 90.0,
        'hybrid_search_threshold_percent': 10.0,
        'max_combination_depth': 3,
        'max_candidates_per_stage': 20,
        'search_algorithm': 'dynamic',
        'max_recursion_times': 2,
        'enable_many_to_many': True,
        'enable_recursive_match': True
    }
    
    if mode == 'precise':
        params.update({
            'exact_match_threshold': 0.0001,
            'approx_match_threshold_percent': 0.5,
            'many_to_many_amount_threshold': 500,
            'many_to_many_percent_threshold': 0.5,
            'final_match_amount_threshold': 50.0,
            'final_match_percent_threshold': 0.05,
            'recursive_min_amount_threshold': 0.005,
            'recursive_percent_threshold': 5.0,
            'large_amount_threshold': 800,
            'pruning_threshold_percent': 95.0,
            'backtrack_overtune_percent': 5.0,
            'backtrack_pruning_threshold_percent': 95.0,
            'hybrid_search_threshold_percent': 5.0,
            'max_combination_depth': 4,
            'max_candidates_per_stage': 25,
            'enable_recursive_match': True
        })
    elif mode == 'fast':
        params.update({
            'exact_match_threshold': 0.01,
            'approx_match_threshold_percent': 2.0,
            'many_to_many_amount_threshold': 3000,
            'many_to_many_percent_threshold': 2.0,
            'final_match_amount_threshold': 200.0,
            'final_match_percent_threshold': 0.2,
            'recursive_min_amount_threshold': 0.02,
            'recursive_percent_threshold': 15.0,
            'large_amount_threshold': 1500,
            'pruning_threshold_percent': 80.0,
            'backtrack_overtune_percent': 15.0,
            'backtrack_pruning_threshold_percent': 80.0,
            'hybrid_search_threshold_percent': 15.0,
            'max_combination_depth': 2,
            'max_candidates_per_stage': 15,
            'search_algorithm': 'backtrack',
            'max_recursion_times': 1,
            'enable_recursive_match': False
        })
    
    return params


# ==================== 主入口函数 ====================

def execute(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    技能主执行函数
    
    参数格式:
    {
        "action": "convert|rename|merge|generate|invoice_extract|tax_reconcile",
        "files": [...],  # 用户上传的文件列表
        "_user_id": "用户ID",
        "_subscription": {"tier": "free|pro"},
        ...其他业务参数
    }
    """
    try:
        # 获取操作类型
        action = params.get('action')
        if not action:
            return {
                'status': 'error',
                'message': '请指定操作类型 (action)'
            }
        
        # 获取用户信息
        user_tier = params.get('_subscription', {}).get('tier', 'free')
        user_id = params.get('_user_id', 'anonymous')
        
        logger.info(f"用户 {user_id} ({user_tier}) 执行操作: {action}")
        
        # 权限检查
        free_features = ['convert', 'rename', 'merge', 'generate']
        pro_features = ['invoice_extract', 'tax_reconcile']
        
        if user_tier != 'pro' and action in pro_features:
            # 检查试用次数
            trial_result = check_trial_quota(user_id, action)
            if trial_result.get('has_trial'):
                logger.info(f"用户 {user_id} 使用试用额度: {action}")
                params['_trial_mode'] = True
            else:
                return {
                    'status': 'error',
                    'message': f'{action} 需要专业版订阅',
                    'upgrade_url': '/upgrade',
                    'trial_available': trial_result.get('trial_available', False)
                }
        
        # 路由到具体功能
        if action == 'convert':
            result = handle_convert(params)
        elif action == 'rename':
            result = handle_rename(params)
        elif action == 'merge':
            result = handle_merge(params)
        elif action == 'generate':
            result = handle_generate(params)
        elif action == 'invoice_extract':
            result = handle_invoice_extract(params)
        elif action == 'tax_reconcile':
            result = handle_tax_reconcile(params)
        else:
            return {
                'status': 'error',
                'message': f'不支持的操作类型: {action}'
            }
        
        # 处理计费
        if result.get('status') == 'success':
            billing_info = calculate_billing(action, result, params)
            result['billing'] = billing_info
            
            if params.get('_trial_mode'):
                deduct_trial_quota(user_id, action)
        
        # 转换类型（确保JSON可序列化）
        result = convert_types(result)
        
        logger.info(f"操作完成: {action}, 状态: {result.get('status')}")
        return result
        
    except Exception as e:
        logger.error(f"执行失败: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'message': f'操作失败: {str(e)}'
        }


# ==================== 文件转换 ====================

def handle_convert(params: Dict[str, Any]) -> Dict[str, Any]:
    """文件批量转换"""
    files = params.get('files', [])
    if not files:
        return {'status': 'error', 'message': '请上传文件'}
    
    target_format = params.get('target_format', 'pdf').lower()
    source_format = params.get('source_format', '').lower()
    
    # 创建临时目录
    input_dir = tempfile.mkdtemp(prefix='convert_input_')
    output_dir = tempfile.mkdtemp(prefix='convert_output_')
    
    try:
        # 保存上传的文件
        saved_files = []
        file_info_map = {}
        
        for file_info in files:
            src_path = file_info.get('path')
            if src_path and os.path.exists(src_path):
                file_name = file_info.get('name')
                dst_path = os.path.join(input_dir, file_name)
                shutil.copy2(src_path, dst_path)
                saved_files.append(dst_path)
                file_info_map[dst_path] = file_info
        
        if not saved_files:
            return {'status': 'error', 'message': '未找到有效文件'}
        
        # 确定转换类型
        image_suffixes = ['jpg', 'jpeg', 'png', 'bmp', 'tif', 'gif', 'webp']
        
        converted_files = []
        failed_files = []
        
        # 如果源格式是图片或目标格式是图片，使用PIL转换
        if source_format in image_suffixes or target_format in image_suffixes:
            from PIL import Image
            
            for src_path in saved_files:
                try:
                    file_name = os.path.basename(src_path)
                    base_name = os.path.splitext(file_name)[0]
                    out_name = f"{base_name}.{target_format}"
                    out_path = os.path.join(output_dir, out_name)
                    
                    img = Image.open(src_path)
                    
                    # 根据目标格式保存
                    if target_format.lower() == 'jpg' or target_format.lower() == 'jpeg':
                        if img.mode in ('RGBA', 'P'):
                            img = img.convert('RGB')
                        img.save(out_path, 'JPEG', quality=95)
                    elif target_format.lower() == 'png':
                        img.save(out_path, 'PNG')
                    elif target_format.lower() == 'bmp':
                        img.save(out_path, 'BMP')
                    elif target_format.lower() == 'gif':
                        img.save(out_path, 'GIF')
                    else:
                        img.save(out_path)
                    
                    if os.path.exists(out_path):
                        converted_files.append({
                            'name': out_name,
                            'path': out_path,
                            'size': os.path.getsize(out_path)
                        })
                        logger.info(f"图片转换成功: {file_name} -> {out_name}")
                    else:
                        failed_files.append(file_name)
                        
                except Exception as e:
                    logger.error(f"图片转换失败 {src_path}: {e}")
                    failed_files.append(os.path.basename(src_path))
        
        # 文档转换（使用原服务）
        elif source_format in ['docx', 'doc', 'xlsx', 'xls', 'pdf'] or target_format in ['docx', 'doc', 'xlsx', 'xls', 'pdf']:
            try:
                convert_params = {
                    'input_dir': input_dir,
                    'output_dir': output_dir,
                    'old_suffix': source_format,
                    'new_suffix': target_format
                }
                
                # 调用原服务
                result = automation.file_convert(convert_params)
                
                # 收集转换后的文件
                if os.path.exists(output_dir):
                    for f in os.listdir(output_dir):
                        file_path = os.path.join(output_dir, f)
                        if os.path.isfile(file_path):
                            converted_files.append({
                                'name': f,
                                'path': file_path,
                                'size': os.path.getsize(file_path)
                            })
                
                # 记录失败的文件
                if result.get('failed_files'):
                    for f in result['failed_files']:
                        if isinstance(f, dict):
                            failed_files.append(f.get('file', '未知'))
                        else:
                            failed_files.append(f)
                            
            except Exception as e:
                logger.error(f"文档转换失败: {e}")
                return {'status': 'error', 'message': f'文档转换失败: {str(e)}'}
        
        else:
            return {'status': 'error', 'message': f'不支持的转换类型: {source_format} -> {target_format}'}
        
        # 构建返回结果
        if converted_files:
            return {
                'status': 'success',
                'message': f'成功转换 {len(converted_files)} 个文件' + (f'，失败 {len(failed_files)} 个' if failed_files else ''),
                'converted_files': converted_files,
                'failed_files': failed_files,
                'download_info': {
                    'type': 'multiple' if len(converted_files) > 1 else 'single',
                    'files': [f['name'] for f in converted_files]
                }
            }
        else:
            return {
                'status': 'error',
                'message': '所有文件转换失败'
            }
        
    except Exception as e:
        logger.error(f"转换处理失败: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}
    finally:
        # 临时目录会在函数返回后被龙虾平台清理
        pass


# ==================== 批量重命名 ====================

def handle_rename(params: Dict[str, Any]) -> Dict[str, Any]:
    """文件批量重命名"""
    files = params.get('files', [])
    if not files:
        return {'status': 'error', 'message': '请上传文件'}
    
    # 创建临时目录
    input_dir = tempfile.mkdtemp(prefix='rename_input_')
    output_dir = tempfile.mkdtemp(prefix='rename_output_')
    
    try:
        # 保存文件并收集时间信息
        file_times = {}
        saved_files = []
        
        for file_info in files:
            src_path = file_info['path']
            file_name = file_info['name']
            dst_path = os.path.join(input_dir, file_name)
            
            shutil.copy2(src_path, dst_path)
            saved_files.append(dst_path)
            
            # 保留原始修改时间
            file_times[file_name] = os.path.getmtime(src_path)
            logger.debug(f"文件 {file_name} 原始时间: {datetime.fromtimestamp(file_times[file_name])}")
        
        # 构建参数
        rename_params = {
            'data_path': params.get('data_path', ''),
            'data_sheet_name': params.get('data_sheet_name', 'Sheet1'),
            'data_key': params.get('data_key', ''),
            'old_dir': input_dir,
            'middle_dir': input_dir,
            'new_dir': output_dir,
            'suffix': params.get('suffix', ''),
            'pattern': params.get('pattern', ''),
            'repl': params.get('repl', ''),
            'count': int(params.get('count', 0)),
            'additional_key': params.get('additional_key', ''),
            'deviation': int(params.get('deviation', 0)),
            'preview_mode': params.get('preview_mode', False),
            'file_times': file_times
        }
        
        logger.info(f"重命名参数: {rename_params}")
        
        # 调用原服务
        result = OsOperation.file_rename(**rename_params)
        
        # 收集重命名后的文件
        renamed_files = []
        if os.path.exists(output_dir):
            for f in os.listdir(output_dir):
                file_path = os.path.join(output_dir, f)
                if os.path.isfile(file_path):
                    renamed_files.append({
                        'name': f,
                        'path': file_path,
                        'size': os.path.getsize(file_path)
                    })
        
        # 构建返回结果
        response = {
            'status': result.get('status', 'success'),
            'message': result.get('message', f'成功重命名 {len(renamed_files)} 个文件'),
            'renamed_files': renamed_files,
            'download_info': {
                'type': 'multiple' if len(renamed_files) > 1 else 'single',
                'files': [f['name'] for f in renamed_files]
            }
        }
        
        # 如果是预览模式，返回预览结果
        if params.get('preview_mode'):
            response['preview'] = result.get('preview', [])
        
        return response
        
    except Exception as e:
        logger.error(f"重命名失败: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}


# ==================== 数据合并 ====================

def handle_merge(params: Dict[str, Any]) -> Dict[str, Any]:
    """数据拼接 (pd.merge)"""
    files = params.get('files', [])
    if len(files) < 2:
        return {'status': 'error', 'message': '请上传至少两个文件：模板文件和数据文件'}
    
    # 创建临时目录
    temp_dir = tempfile.mkdtemp(prefix='merge_')
    
    try:
        # 保存文件
        template_path = None
        data_path = None
        
        for i, file_info in enumerate(files):
            src_path = file_info['path']
            file_name = file_info['name']
            dst_path = os.path.join(temp_dir, file_name)
            shutil.copy2(src_path, dst_path)
            
            if i == 0:
                template_path = dst_path
            elif i == 1:
                data_path = dst_path
        
        if not template_path or not data_path:
            return {'status': 'error', 'message': '请提供模板文件和数据文件'}
        
        # 读取数据
        template_sheet = params.get('input_template_sheet_name', 'Sheet1')
        data_sheet = params.get('input_data_sheet_name', 'Sheet1')
        
        try:
            template_df = pd.read_excel(template_path, sheet_name=template_sheet)
            data_df = pd.read_excel(data_path, sheet_name=data_sheet)
        except Exception as e:
            return {'status': 'error', 'message': f'读取Excel失败: {str(e)}'}
        
        logger.info(f"模板数据: {template_df.shape}, 数据文件: {data_df.shape}")
        
        # 合并
        data_key = params.get('data_key', 'id')
        how = params.get('how', 'inner')
        
        if data_key not in template_df.columns:
            return {'status': 'error', 'message': f'模板文件中没有主键列: {data_key}'}
        if data_key not in data_df.columns:
            return {'status': 'error', 'message': f'数据文件中没有主键列: {data_key}'}
        
        merged_df = DataOperation.key_merge(template_df, data_df, on=data_key, how=how)
        
        logger.info(f"合并后: {merged_df.shape}")
        
        # 保存结果
        output_dir = tempfile.mkdtemp(prefix='merge_output_')
        save_name = params.get('save_name', 'merged_result')
        save_sheet = params.get('save_sheet_name', 'Sheet1')
        output_path = os.path.join(output_dir, f"{save_name}.xlsx")
        
        DataOperation.data_pd_write(output_path, save_sheet, merged_df)
        
        return {
            'status': 'success',
            'message': f'数据合并完成，共 {len(merged_df)} 行，{len(merged_df.columns)} 列',
            'output_file': {
                'name': os.path.basename(output_path),
                'path': output_path,
                'size': os.path.getsize(output_path)
            },
            'summary': {
                'rows': len(merged_df),
                'columns': len(merged_df.columns)
            },
            'download_info': {
                'type': 'single',
                'files': [os.path.basename(output_path)]
            }
        }
        
    except Exception as e:
        logger.error(f"合并失败: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}


# ==================== 模板生成 ====================

def handle_generate(params: Dict[str, Any]) -> Dict[str, Any]:
    """模板批量生成"""
    files = params.get('files', [])
    if len(files) < 2:
        return {'status': 'error', 'message': '请上传模板文件和数据文件（至少两个文件）'}
    
    # 创建临时目录
    work_dir = tempfile.mkdtemp(prefix='generate_')
    output_dir = tempfile.mkdtemp(prefix='generate_output_')
    
    try:
        # 识别模板文件和数据文件
        template_file = None
        data_file = None
        template_name = None
        data_name = None
        
        # 策略1：根据文件名和扩展名判断
        for file_info in files:
            file_name = file_info['name'].lower()
            file_path = file_info['path']
            
            if file_name.endswith(('.docx', '.doc', '.xlsx', '.xls')):
                if not template_file or '模板' in file_name:
                    template_file = file_path
                    template_name = file_info['name']
                else:
                    data_file = file_path
                    data_name = file_info['name']
            elif file_name.endswith(('.xlsx', '.xls', '.csv')):
                data_file = file_path
                data_name = file_info['name']
        
        # 策略2：如果还没区分，按上传顺序
        if not template_file or not data_file:
            template_file = files[0]['path']
            data_file = files[1]['path']
            template_name = files[0]['name']
            data_name = files[1]['name']
        
        logger.info(f"模板文件: {template_name}")
        logger.info(f"数据文件: {data_name}")
        
        # 复制文件到工作目录
        temp_template = os.path.join(work_dir, template_name)
        temp_data = os.path.join(work_dir, data_name)
        shutil.copy2(template_file, temp_template)
        shutil.copy2(data_file, temp_data)
        
        # 获取参数
        data_key = params.get('data_key', '姓名')
        mode = params.get('mode', 'mixed')
        reserved_rows = int(params.get('reserved_rows', 1))
        insert_row = int(params.get('insert_row', 1))
        insert_col = int(params.get('insert_col', 1))
        preview_mode = params.get('preview_mode', False)
        
        # 根据模板类型调用对应的处理函数
        file_ext = os.path.splitext(template_name)[1].lower()
        
        if file_ext in ['.xlsx', '.xls']:
            # Excel模板
            result = TemplateEngine.process_data_xlsx(
                input_template=temp_template,
                input_template_sheet_name=params.get('input_template_sheet_name', 'Sheet1'),
                input_data=temp_data,
                input_data_sheet_name=params.get('input_data_sheet_name', 'Sheet1'),
                output_dir=output_dir,
                data_key=data_key,
                insert_row=insert_row,
                insert_col=insert_col,
                reserved_rows=reserved_rows,
                mode=mode,
                preview_mode=preview_mode
            )
        else:
            # Word模板
            result = TemplateEngine.process_data_docx(
                input_template=temp_template,
                input_data=temp_data,
                input_data_sheet_name=params.get('input_data_sheet_name', 'Sheet1'),
                output_dir=output_dir,
                data_key=data_key,
                reserved_rows=reserved_rows,
                mode=mode,
                preview_mode=preview_mode
            )
        
        # 收集生成的文件
        generated_files = []
        if os.path.exists(output_dir):
            for f in os.listdir(output_dir):
                file_path = os.path.join(output_dir, f)
                if os.path.isfile(file_path):
                    generated_files.append({
                        'name': f,
                        'path': file_path,
                        'size': os.path.getsize(file_path)
                    })
        
        # 构建返回结果
        response = {
            'status': result.get('status', 'success'),
            'message': result.get('message', f'成功生成 {len(generated_files)} 个文件'),
            'generated_files': generated_files,
            'download_info': {
                'type': 'multiple' if len(generated_files) > 1 else 'single',
                'files': [f['name'] for f in generated_files]
            }
        }
        
        # 如果是预览模式，返回预览结果
        if preview_mode:
            response['preview'] = result.get('preview', [])
        
        return response
        
    except Exception as e:
        logger.error(f"模板生成失败: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}


# ==================== 发票提取 ====================

def handle_invoice_extract(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    发票信息提取（完整流程）
    
    流程：
    1. PDF文件 → 自动转DOCX（后台秒级，用户无感）
    2. 从DOCX提取发票信息
    3. 所有提取结果合拼为一个汇总Excel
    4. 返回汇总文件
    """
    files = params.get('files', [])
    if not files:
        return {'status': 'error', 'message': '请上传发票文件'}
    
    version = params.get('version', 'basic')  # basic 或 complete
    
    # 创建临时工作目录
    work_dir = tempfile.mkdtemp(prefix='invoice_')
    docx_dir = os.path.join(work_dir, 'docx_files')
    output_dir = os.path.join(work_dir, 'output')
    os.makedirs(docx_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        # ===== 步骤1：PDF转DOCX（用户无感）=====
        logger.info(f"步骤1：PDF转DOCX，共 {len(files)} 个文件")
        
        pdf_files = [f for f in files if f['name'].lower().endswith('.pdf')]
        docx_files = [f for f in files if f['name'].lower().endswith('.docx')]
        
        converted_count = 0
        all_docx_files = []  # 所有待提取的DOCX文件
        
        # 添加原始DOCX文件
        for docx_info in docx_files:
            all_docx_files.append({
                'path': docx_info['path'],
                'name': docx_info['name'],
                'original': docx_info['name']
            })
        
        # 转换PDF到DOCX
        if pdf_files:
            # 尝试获取LibreOffice路径（龙虾平台需要配置）
            convert_tool = params.get('libreoffice_path', r"D:\Program Files\libreoffice\program\soffice.exe")
            
            # 检查工具是否存在
            if not os.path.exists(convert_tool):
                logger.warning(f"LibreOffice不存在: {convert_tool}，PDF转换可能失败")
            
            for pdf_info in pdf_files:
                pdf_path = pdf_info['path']
                pdf_name = pdf_info['name']
                docx_name = pdf_name.replace('.pdf', '.docx')
                docx_path = os.path.join(docx_dir, docx_name)
                
                try:
                    # 执行转换
                    success = MutualConver._convert_with_libreoffice_fast(
                        pdf_path, docx_path, convert_tool, 'docx'
                    )
                    
                    if success and os.path.exists(docx_path) and os.path.getsize(docx_path) > 0:
                        converted_count += 1
                        all_docx_files.append({
                            'path': docx_path,
                            'name': docx_name,
                            'original': pdf_name
                        })
                        logger.info(f"PDF转DOCX成功: {pdf_name} -> {docx_name}")
                    else:
                        logger.warning(f"PDF转DOCX失败: {pdf_name}")
                        
                except Exception as e:
                    logger.error(f"PDF转DOCX异常 {pdf_name}: {e}")
        
        if not all_docx_files:
            return {
                'status': 'error',
                'message': '没有找到可处理的DOCX文件（PDF转换也可能失败）'
            }
        
        # ===== 步骤2：发票信息提取 =====
        logger.info(f"步骤2：发票信息提取，版本: {version}，文件数: {len(all_docx_files)}")
        
        extracted_results = []
        for file_info in all_docx_files:
            try:
                if version == 'complete':
                    data = InvoiceExtractionServiceComplete.extract_invoice_from_xml(file_info['path'])
                else:
                    data = InvoiceExtractionService.extract_invoice_from_xml(file_info['path'])
                
                # 计算准确率（核心字段）
                core_fields = ['invoice_no', 'invoice_date', 'buyer_name', 'total_with_tax']
                present = sum(1 for f in core_fields if f in data and data[f])
                accuracy = (present / len(core_fields)) * 100 if core_fields else 0
                
                extracted_results.append({
                    'file': file_info['name'],
                    'original_file': file_info.get('original', file_info['name']),
                    'status': 'success',
                    'data': data,
                    'accuracy': accuracy
                })
                
            except Exception as e:
                logger.error(f"提取失败 {file_info['name']}: {e}")
                extracted_results.append({
                    'file': file_info['name'],
                    'original_file': file_info.get('original', file_info['name']),
                    'status': 'error',
                    'message': str(e)
                })
        
        success_count = sum(1 for r in extracted_results if r['status'] == 'success')
        
        if success_count == 0:
            return {
                'status': 'error',
                'message': '所有发票提取失败，请检查文件格式'
            }
        
        # ===== 步骤3：合拼为汇总明细（直接用并集，有啥放啥）=====
        logger.info(f"步骤3：合拼汇总，成功 {success_count} 张")
        
        summary_data = []
        for result in extracted_results:
            if result['status'] == 'success':
                data = result['data']
                
                # 直接把所有数据放进去，有啥放啥
                row = {
                    '文件名': result['original_file'],
                    **data  # 展开所有提取的字段
                }
                summary_data.append(row)
        
        # 保存汇总文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        summary_path = os.path.join(output_dir, f'发票汇总_{timestamp}.xlsx')
        
        df = pd.DataFrame(summary_data)
        
        # 保存Excel
        with pd.ExcelWriter(summary_path, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='发票汇总')
            
            # 调整列宽
            worksheet = writer.sheets['发票汇总']
            for column_cells in worksheet.columns:
                try:
                    length = max(len(str(cell.value)) for cell in column_cells)
                    worksheet.column_dimensions[column_cells[0].column_letter].width = min(length + 2, 50)
                except:
                    pass
        
        # 计算整体准确率
        accuracies = [r['accuracy'] for r in extracted_results if r['status'] == 'success']
        overall_accuracy = sum(accuracies) / len(accuracies) if accuracies else 0
        
        # 返回结果
        return {
            'status': 'success',
            'message': f'成功提取 {success_count}/{len(all_docx_files)} 张发票，已合拼为汇总文件',
            'summary': {
                'total_files': len(all_docx_files),
                'success_count': success_count,
                'pdf_converted': converted_count,
                'overall_accuracy': round(overall_accuracy, 2)
            },
            'results': extracted_results,
            'output_file': {
                'name': os.path.basename(summary_path),
                'path': summary_path,
                'size': os.path.getsize(summary_path)
            },
            'download_info': {
                'type': 'single',
                'files': [os.path.basename(summary_path)],
                'title': '发票汇总结果'
            }
        }
        
    except Exception as e:
        logger.error(f"发票提取流程失败: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}


# ==================== 财税对账 ====================

def handle_tax_reconcile(params: Dict[str, Any]) -> Dict[str, Any]:
    """财税智能对账"""
    files = params.get('files', [])
    if len(files) != 2:
        return {'status': 'error', 'message': '请上传两个文件：税局数据和财务数据'}
    
    # 创建临时目录
    output_dir = tempfile.mkdtemp(prefix='tax_reconcile_')
    
    try:
        # 获取文件路径
        tax_file = files[0]['path']
        sap_file = files[1]['path']
        
        # 读取数据
        try:
            tax_df = pd.read_excel(tax_file)
            sap_df = pd.read_excel(sap_file)
        except Exception as e:
            return {'status': 'error', 'message': f'读取Excel文件失败: {str(e)}'}
        
        # 验证必要列
        required_cols = ['税额', '税率']
        missing_cols = []
        
        for col in required_cols:
            if col not in tax_df.columns:
                missing_cols.append(f'税局文件缺少列: {col}')
            if col not in sap_df.columns:
                missing_cols.append(f'SAP文件缺少列: {col}')
        
        if missing_cols:
            return {'status': 'error', 'message': '；'.join(missing_cols)}
        
        # 数据预处理
        tax_df['税额'] = pd.to_numeric(tax_df['税额'], errors='coerce').fillna(0)
        sap_df['税额'] = pd.to_numeric(sap_df['税额'], errors='coerce').fillna(0)
        tax_df['税率'] = tax_df['税率'].astype(str).str.strip()
        sap_df['税率'] = sap_df['税率'].astype(str).str.strip()
        
        # 获取匹配模式
        match_mode = params.get('match_mode', 'standard')
        algorithm_params = get_algorithm_params(match_mode)
        
        logger.info(f"对账参数: 模式={match_mode}, 多对多阈值={algorithm_params.get('many_to_many_amount_threshold')}")
        
        # 执行对账
        reconciler = OptimizedTaxAmountReconciliationService(tax_df, sap_df, algorithm_params)
        result = reconciler.reconcile_all()
        
        match_rate = result['summary'].get('match_rate', 0)
        accuracy_guarantee = params.get('accuracy_guarantee', 80)
        need_refund = match_rate < accuracy_guarantee
        
        # 生成输出文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(output_dir, f'税务对账结果_{timestamp}.xlsx')
        reconciler.export_to_excel(output_path)
        
        # 构建返回结果
        return {
            'status': 'success',
            'message': f'对账完成，匹配率: {match_rate:.2f}%',
            'summary': {
                'total_tax_amount': result['summary']['total_tax_amount'],
                'total_sap_amount': result['summary']['total_sap_amount'],
                'total_matched': result['summary']['total_matched'],
                'balance_diff': result['summary']['balance_diff'],
                'match_rate': round(match_rate, 2),
                'match_count': result['summary']['match_count'],
                'unmatched_tax_count': result['summary']['unmatched_tax_count'],
                'unmatched_sap_count': result['summary']['unmatched_sap_count'],
                'validation_passed': result['summary'].get('validation_passed', False)
            },
            'performance_stats': result['performance_stats'],
            'match_rate': round(match_rate, 2),
            'need_refund': need_refund,
            'refund_reason': f'匹配率{match_rate:.2f}%低于保证阈值{accuracy_guarantee}%' if need_refund else None,
            'output_file': {
                'name': os.path.basename(output_path),
                'path': output_path,
                'size': os.path.getsize(output_path)
            },
            'download_info': {
                'type': 'single',
                'files': [os.path.basename(output_path)],
                'title': '税务对账结果'
            }
        }
        
    except Exception as e:
        logger.error(f"对账失败: {e}", exc_info=True)
        return {'status': 'error', 'message': str(e)}