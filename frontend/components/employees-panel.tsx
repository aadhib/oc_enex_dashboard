"use client";

type EmployeeItem = {
  emp_id: number | string;
  employee_id?: number | string;
  card_no: string;
  employee_name: string;
  department?: string | null;
};

type EmployeesPanelProps = {
  employees: EmployeeItem[];
  selectedCardNo: string;
  search: string;
  employeeLoading: boolean;
  onSearchChange: (value: string) => void;
  onSelectCardNo: (cardNo: string) => void;
};

export default function EmployeesPanel({
  employees,
  selectedCardNo,
  search,
  employeeLoading,
  onSearchChange,
  onSelectCardNo
}: EmployeesPanelProps) {
  return (
    <div className="h-full min-h-0 overflow-hidden flex flex-col rounded-2xl border border-zinc-800 bg-zinc-900/70 p-4 shadow-xl backdrop-blur">
      <div className="shrink-0">
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-lg font-medium text-zinc-100">Employees</h2>
          {employeeLoading ? <span className="text-xs text-zinc-400">Loading...</span> : null}
        </div>

        <input
          value={search}
          onChange={(event) => onSearchChange(event.target.value)}
          placeholder="Search by Name, Card No or EmpID"
          className="mb-3 w-full rounded-lg border border-zinc-700 bg-zinc-950 px-3 py-2 text-sm text-zinc-100 outline-none transition focus:border-cyan-400"
        />
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto pr-1 space-y-2 table-scroll">
        {employees.map((employee) => {
          const active = employee.card_no === selectedCardNo;
          const employeeId = String(employee.employee_id ?? employee.emp_id ?? "").trim();
          const department = String(employee.department ?? "").trim();
          const detailParts = [employee.card_no];
          if (employeeId) {
            detailParts.push(`EmpID: ${employeeId}`);
          }
          if (department) {
            detailParts.push(department);
          }
          return (
            <button
              key={`${employee.emp_id}-${employee.card_no}`}
              type="button"
              onClick={() => onSelectCardNo(employee.card_no)}
              className={`w-full rounded-xl border px-3 py-2 text-left text-sm transition ${
                active
                  ? "border-cyan-400 bg-cyan-500/20 text-cyan-100"
                  : "border-zinc-800 bg-zinc-900 text-zinc-300 hover:border-zinc-600"
              }`}
            >
              <p className="font-medium leading-tight">{employee.employee_name}</p>
              <p className={`mt-1 text-xs ${active ? "text-cyan-200/80" : "text-zinc-500"}`}>{detailParts.join(" • ")}</p>
            </button>
          );
        })}
        {!employeeLoading && !employees.length ? (
          <p className="rounded-lg border border-dashed border-zinc-800 p-3 text-sm text-zinc-500">No employees found.</p>
        ) : null}
      </div>
    </div>
  );
}
